"""
Telegram-бот для проекта iis_tracker.

Функции:
- Команды:
    /today      — сводка по портфелю "Семейный капитал" на сегодня
    /week       — сводка по текущей неделе
    /month      — отчёт по текущему месяцу
    /year       — отчёт за год (YTD или календарный)
    /dataset    — архив json+csv+md для AI-анализа
    /structure  — текущая структура портфеля
    /history    — график стоимости портфеля и суммы пополнений
    /twr        — TWR, XIRR и run-rate на конец года + график по дням
    /help       — список команд

- Ежедневная задача (18:00 по времени хоста, через JobQueue):
    * по пятницам — недельный отчёт (/week)
    * в последний день месяца — отчёт за месяц (/month)
    * триггеры:
        - новый максимум портфеля
        - годовой план по пополнениям выполнен (400k за год)
    * (ежедневная сводка /today автоматически НЕ отправляется)

Безопасность:
- ALLOWED_USER_IDS — белый список Telegram user_id.
- Все остальные пользователи игнорируются.
"""

import os
import tempfile
import json
import zipfile
from decimal import Decimal
from datetime import datetime, date, time, timedelta, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

from telegram import Update, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from queries import (
    get_asset_alias_rows,
    get_daily_snapshot_rows,
    get_dataset_bounds,
    get_dataset_operations,
    get_deposits_by_date,
    get_deposits_sum_for_period,
    get_first_snapshot_in_period,
    get_income_events_for_period,
    get_last_snapshot_before_date,
    get_latest_snapshot_with_id,
    get_monthly_deposits,
    get_monthly_portfolio_values,
    get_pending_invest_notifications,
    get_portfolio_timeseries,
    get_positions_for_snapshot,
    get_unnotified_income_events,
    mark_income_event_notified,
    mark_invest_notification_sent,
    resolve_reporting_account_id,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    HOST_TZ,
    INVEST_USAGE_TEXT,
    JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
    JOBQUEUE_SMOKE_TEST_ON_START,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    SHORT_MONTHS_RU,
    TARGETS_USAGE_TEXT,
    TARGET_CHAT_IDS,
    TELEGRAM_BOT_TOKEN,
    TZ,
    TZ_NAME,
    db_session,
    decimal_to_str,
    fmt_compact_pct,
    fmt_compact_rub,
    fmt_decimal_rub,
    fmt_plain_pct,
    fmt_signed_amount,
    is_authorized,
    json_default,
    last_day_of_month,
    log_update_received,
    logger,
    normalize_decimal,
    safe_send_message,
    to_iso_datetime,
    to_local_market_date,
    write_csv_file,
)
from services import (
    build_asset_alias_lookup,
    build_help_text,
    build_invest_text_for_account,
    build_logical_asset_id,
    build_month_summary,
    build_rebalance_text_for_account,
    build_reconciliation_by_asset_type,
    build_structure_text,
    build_targets_text_for_account,
    build_today_summary,
    build_triggers_messages,
    build_week_summary,
    build_year_summary,
    classify_operation_group,
    compute_portfolio_xirr_and_run_rate,
    compute_twr_timeseries,
    get_year_period,
    is_income_event_backed_tax_operation,
    parse_rebalance_targets_args,
    render_twr_summary_text,
    replace_rebalance_targets,
)

CHART_COLORS = {
    "portfolio": "#1f6f8b",
    "portfolio_fill": "#d9eef4",
    "deposits": "#d8a25e",
    "deposits_fill": "#f6e4c9",
    "twr": "#6b7aa1",
    "positive": "#3e8e63",
    "positive_fill": "#dcefe3",
    "negative": "#c46b4f",
    "negative_fill": "#f5dfd7",
    "neutral": "#8f97a6",
    "grid": "#d8dfe7",
    "spine": "#d6dce3",
    "text": "#1f2933",
    "muted": "#67707b",
}

# ==========================================

from common.text_utils import has_mojibake

# =============== HELPERS ==================


async def debug_command_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_name = None
    text = getattr(update.effective_message, "text", None) or ""
    if text.startswith("/"):
        command_name = text.split()[0]
    log_update_received(update, command_name=command_name)


def format_month_short_label(d: date) -> str:
    return SHORT_MONTHS_RU[d.month]


def format_day_month_label(d: date, include_year: bool = False) -> str:
    label = f"{d.day} {format_month_short_label(d)}"
    if include_year:
        return f"{label}\n{d.year}"
    return label


def build_month_tick_labels(months: list[date]) -> list[str]:
    if not months:
        return []

    multi_year = len({month.year for month in months}) > 1
    labels: list[str] = []
    prev_year: int | None = None

    for month in months:
        label = format_month_short_label(month)
        if multi_year and month.year != prev_year:
            label = f"{label}\n{month.year}"
        labels.append(label)
        prev_year = month.year

    return labels


def pick_tick_indices(length: int, max_ticks: int = 7) -> list[int]:
    if length <= 0:
        return []
    if length <= max_ticks:
        return list(range(length))

    indices: list[int] = []
    for step in range(max_ticks):
        idx = round(step * (length - 1) / (max_ticks - 1))
        if idx not in indices:
            indices.append(idx)
    return indices


def build_date_ticks(dates: list[date], max_ticks: int = 7) -> tuple[list[date], list[str]]:
    indices = pick_tick_indices(len(dates), max_ticks=max_ticks)
    selected_dates = [dates[idx] for idx in indices]

    labels: list[str] = []
    prev_year: int | None = None
    for dt in selected_dates:
        include_year = prev_year is None or dt.year != prev_year
        labels.append(format_day_month_label(dt, include_year=include_year))
        prev_year = dt.year

    return selected_dates, labels


def rub_axis_formatter(value: float, _pos: int | None = None) -> str:
    return fmt_compact_rub(float(value), precision=1)


def pct_axis_formatter(value: float, _pos: int | None = None) -> str:
    return fmt_compact_pct(float(value), precision=0)


def set_chart_header(fig, title: str, subtitle: str | None = None):
    fig.patch.set_facecolor("white")
    fig.suptitle(
        title,
        x=0.125,
        y=0.972,
        ha="left",
        fontsize=14,
        fontweight="bold",
        color=CHART_COLORS["text"],
    )
    if subtitle:
        fig.text(
            0.125,
            0.905,
            subtitle,
            ha="left",
            va="top",
            fontsize=9,
            color=CHART_COLORS["muted"],
        )


def apply_chart_style(ax, y_formatter=None):
    ax.set_facecolor("white")
    ax.grid(axis="y", color=CHART_COLORS["grid"], linewidth=0.8, alpha=0.7)
    ax.grid(axis="x", visible=False)
    ax.tick_params(axis="both", labelsize=9, colors=CHART_COLORS["muted"])
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))

    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(CHART_COLORS["spine"])
    ax.spines["bottom"].set_color(CHART_COLORS["spine"])

    if y_formatter is not None:
        ax.yaxis.set_major_formatter(FuncFormatter(y_formatter))


def annotate_series_last_point(
    ax,
    x_values: list,
    y_values: list[float],
    label: str,
    color: str,
    y_offset: int = 0,
):
    if not x_values or not y_values:
        return

    last_x = x_values[-1]
    last_y = y_values[-1]
    ax.scatter([last_x], [last_y], color=color, s=28, zorder=5)
    ax.annotate(
        label,
        xy=(last_x, last_y),
        xytext=(10, y_offset),
        textcoords="offset points",
        ha="left",
        va="center",
        fontsize=9,
        color=CHART_COLORS["text"],
        bbox={
            "boxstyle": "round,pad=0.3",
            "fc": "white",
            "ec": color,
            "lw": 1,
            "alpha": 0.96,
        },
        clip_on=False,
        zorder=6,
    )


def annotate_bar_values(ax, x_values: list[int], values: list[float], formatter, text_color: str | None = None):
    visible_values = [abs(value) for value in values if value is not None]
    if not visible_values:
        return

    offset = max(max(visible_values) * 0.04, 1.0)
    for x, value in zip(x_values, values):
        if value is None:
            continue

        label_y = value + offset if value >= 0 else value - offset
        va = "bottom" if value >= 0 else "top"
        color = text_color
        if color is None:
            if value > 0:
                color = CHART_COLORS["positive"]
            elif value < 0:
                color = CHART_COLORS["negative"]
            else:
                color = CHART_COLORS["neutral"]

        ax.text(
            x,
            label_y,
            formatter(value),
            ha="center",
            va=va,
            fontsize=8,
            color=color,
        )


def set_value_axis_limits(
    ax,
    values: list[float],
    min_padding_ratio: float = 0.12,
    flat_padding_ratio: float = 0.05,
):
    if not values:
        return

    min_value = min(values)
    max_value = max(values)
    span = max_value - min_value

    if span <= 0:
        pad = max(abs(max_value) * flat_padding_ratio, 1.0)
        ax.set_ylim(min_value - pad, max_value + pad)
        return

    pad = max(span * min_padding_ratio, 1.0)
    ax.set_ylim(min_value - pad, max_value + pad)



























































def build_history_chart(path: str) -> str | None:
    """
    Строит PNG-график стоимости портфеля и накопленных пополнений.
    Ось X — понедельная разметка.
    """
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        ts = get_portfolio_timeseries(session, account_id)
        deps = get_deposits_by_date(session, account_id)

    if len(ts) < 2:
        return None

    # Сортируем исходные данные (на всякий случай)
    ts_sorted = sorted(ts, key=lambda x: x["snapshot_date"])
    deps_sorted = sorted(deps, key=lambda x: x["d"])

    start_date = ts_sorted[0]["snapshot_date"]
    end_date = ts_sorted[-1]["snapshot_date"]

    # Генерируем даты с шагом 1 неделя
    week_dates = []
    curr = start_date
    while curr <= end_date:
        week_dates.append(curr)
        curr += timedelta(days=7)
    
    # Если последняя точка далеко от end_date, можно добавить и end_date
    if week_dates[-1] < end_date:
        week_dates.append(end_date)

    # Подготавливаем массивы значений для графика
    values = []
    cum_deps = []

    # Превращаем ts_sorted в список кортежей для удобства
    ts_data = [(row["snapshot_date"], float(row["total_value"])) for row in ts_sorted]
    # Превращаем deps_sorted в список кортежей
    deps_data = [(row["d"], float(row["s"])) for row in deps_sorted]

    for d in week_dates:
        # 1. Стоимость портфеля на дату d (берем последний снапшот <= d)
        val = 0.0
        relevant_snaps = [v for (dt, v) in ts_data if dt <= d]
        if relevant_snaps:
            val = relevant_snaps[-1]
        values.append(val)

        # 2. Кумулятивные пополнения на дату d (сумма всех депозитов <= d)
        relevant_deps = [amt for (dt, amt) in deps_data if dt <= d]
        total_d = sum(relevant_deps)
        cum_deps.append(total_d)

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(
        fig,
        f"{ACCOUNT_FRIENDLY_NAME}: портфель и пополнения",
        "Разница между линиями показывает результат сверх пополнений.",
    )
    apply_chart_style(ax, rub_axis_formatter)

    if cum_deps:
        positive_gap = [value >= dep for value, dep in zip(values, cum_deps)]
        negative_gap = [value < dep for value, dep in zip(values, cum_deps)]
        ax.fill_between(
            week_dates,
            values,
            cum_deps,
            where=positive_gap,
            color=CHART_COLORS["positive_fill"],
            alpha=0.8,
            interpolate=True,
            zorder=1,
        )
        ax.fill_between(
            week_dates,
            values,
            cum_deps,
            where=negative_gap,
            color=CHART_COLORS["negative_fill"],
            alpha=0.8,
            interpolate=True,
            zorder=1,
        )
        ax.plot(
            week_dates,
            cum_deps,
            color=CHART_COLORS["deposits"],
            linewidth=2.0,
            linestyle=(0, (4, 3)),
            zorder=2,
        )

    ax.plot(
        week_dates,
        values,
        color=CHART_COLORS["portfolio"],
        linewidth=2.6,
        zorder=3,
    )

    tick_dates, tick_labels = build_date_ticks(week_dates, max_ticks=7)
    ax.set_xticks(tick_dates)
    ax.set_xticklabels(tick_labels)
    ax.set_ylabel("Стоимость")
    ax.margins(x=0.03, y=0.14)

    portfolio_offset = 12
    deposits_offset = -14 if values[-1] >= cum_deps[-1] else 12
    if abs(values[-1] - cum_deps[-1]) < max(values[-1], cum_deps[-1], 1.0) * 0.07:
        portfolio_offset = 18
        deposits_offset = -18

    annotate_series_last_point(
        ax,
        week_dates,
        values,
        f"Портфель {fmt_compact_rub(values[-1])}",
        CHART_COLORS["portfolio"],
        y_offset=portfolio_offset,
    )
    if cum_deps:
        annotate_series_last_point(
            ax,
            week_dates,
            cum_deps,
            f"Пополнения {fmt_compact_rub(cum_deps[-1])}",
            CHART_COLORS["deposits"],
            y_offset=deposits_offset,
        )

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_year_chart(path: str, year: int, end_date_exclusive: date) -> str | None:
    year_start = date(year, 1, 1)
    period_start_dt = datetime.combine(year_start, time.min)
    period_end_dt_exclusive = datetime.combine(end_date_exclusive, time.min)
    is_ytd = end_date_exclusive.year == year and end_date_exclusive <= (datetime.now(TZ).date() + timedelta(days=1))

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        portfolio_rows = get_monthly_portfolio_values(session, account_id, period_start_dt, period_end_dt_exclusive, is_ytd)
        deposits_rows = get_monthly_deposits(session, account_id, period_start_dt, period_end_dt_exclusive)

    if not portfolio_rows:
        return None

    portfolio_by_month = {
        row["month_start"]: float(row["total_value"] or 0)
        for row in portfolio_rows
    }
    deposits_by_month = {
        row["month_start"]: float(row["amount"] or 0)
        for row in deposits_rows
    }

    months = sorted(portfolio_by_month.keys())
    portfolio_values = [portfolio_by_month[m] for m in months]
    deposits_values = [deposits_by_month.get(m, 0.0) for m in months]

    x = list(range(len(months)))
    chart_title = f"{year} YTD: как рос портфель" if is_ytd else f"{year}: как рос портфель"
    has_deposits = any(value != 0 for value in deposits_values)
    chart_subtitle = (
        "Сверху — стоимость на конец месяца, снизу — пополнения за месяц."
        if has_deposits
        else "Пополнений за этот период не было, поэтому показана только динамика портфеля."
    )

    if has_deposits:
        fig, (ax_portfolio, ax_deposits) = plt.subplots(
            2,
            1,
            figsize=(10.5, 5.9),
            sharex=True,
            gridspec_kw={"height_ratios": [2.2, 1.25]},
        )
    else:
        fig, ax_portfolio = plt.subplots(figsize=(10.5, 4.6))
        ax_deposits = None
    set_chart_header(
        fig,
        chart_title,
        chart_subtitle,
    )

    apply_chart_style(ax_portfolio, rub_axis_formatter)
    if ax_deposits is not None:
        apply_chart_style(ax_deposits, rub_axis_formatter)

    if ax_deposits is not None:
        ax_portfolio.fill_between(x, portfolio_values, color=CHART_COLORS["portfolio_fill"], alpha=0.65, zorder=1)
    ax_portfolio.plot(
        x,
        portfolio_values,
        color=CHART_COLORS["portfolio"],
        linewidth=2.6,
        marker="o",
        markersize=4,
        zorder=3,
    )
    ax_portfolio.set_ylabel("Портфель")
    ax_portfolio.margins(x=0.04)
    if ax_deposits is None:
        set_value_axis_limits(ax_portfolio, portfolio_values, min_padding_ratio=0.18, flat_padding_ratio=0.03)
    else:
        ax_portfolio.margins(y=0.16)
    annotate_series_last_point(
        ax_portfolio,
        x,
        portfolio_values,
        f"{fmt_compact_rub(portfolio_values[-1])}",
        CHART_COLORS["portfolio"],
        y_offset=12,
    )

    labels = build_month_tick_labels(months)
    if ax_deposits is not None:
        ax_deposits.bar(
            x,
            deposits_values,
            width=0.58,
            color=CHART_COLORS["deposits"],
            edgecolor="none",
            zorder=3,
        )
        ax_deposits.set_ylabel("Пополнения")
        ax_deposits.margins(x=0.04)
        max_deposit = max(deposits_values) if deposits_values else 0.0
        upper_limit = max_deposit * 1.2 if max_deposit > 0 else 1.0
        ax_deposits.set_ylim(0, upper_limit)
        annotate_bar_values(
            ax_deposits,
            x,
            deposits_values,
            lambda value: fmt_compact_rub(value, precision=0),
            text_color=CHART_COLORS["muted"],
        )
        ax_deposits.set_xticks(x)
        ax_deposits.set_xticklabels(labels)
        fig.tight_layout(rect=(0, 0, 1, 0.86), h_pad=1.2)
    else:
        ax_portfolio.set_xticks(x)
        ax_portfolio.set_xticklabels(labels)
        fig.tight_layout(rect=(0, 0, 1, 0.83))

    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_year_monthly_delta_chart(path: str, year: int, end_date_exclusive: date) -> str | None:
    year_start = date(year, 1, 1)
    period_start_dt = datetime.combine(year_start, time.min)
    period_end_dt_exclusive = datetime.combine(end_date_exclusive, time.min)
    is_ytd = end_date_exclusive.year == year and end_date_exclusive <= (datetime.now(TZ).date() + timedelta(days=1))

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        portfolio_rows = get_monthly_portfolio_values(session, account_id, period_start_dt, period_end_dt_exclusive, is_ytd)
        deposits_rows = get_monthly_deposits(session, account_id, period_start_dt, period_end_dt_exclusive)

        if not portfolio_rows:
            return None

        deposits_by_month = {
            row["month_start"]: float(row["amount"] or 0)
            for row in deposits_rows
        }

        months = [row["month_start"] for row in portfolio_rows]
        values = [float(row["total_value"] or 0) for row in portfolio_rows]

        delta_points: list[tuple[date, float]] = []
        first_month_start = months[0]
        first_month_end_exclusive = (
            date(first_month_start.year + 1, 1, 1)
            if first_month_start.month == 12
            else date(first_month_start.year, first_month_start.month + 1, 1)
        )

        first_snapshot = get_first_snapshot_in_period(session, account_id, first_month_start, first_month_end_exclusive)
        first_month_base = float(first_snapshot["total_value"] or 0) if first_snapshot is not None else values[0]
        first_period_start = first_snapshot["snapshot_date"] if first_snapshot is not None else first_month_start

        if first_month_start.month == 1:
            prev_snapshot = get_last_snapshot_before_date(session, account_id, first_month_start)
            if prev_snapshot is not None:
                first_month_base = float(prev_snapshot["total_value"] or 0)
                first_period_start = first_month_start

        if first_period_start == first_month_start:
            first_month_deposits = deposits_by_month.get(first_month_start, 0.0)
        else:
            first_month_deposits = get_deposits_sum_for_period(
                session,
                account_id,
                datetime.combine(first_period_start, time.min),
                datetime.combine(first_month_end_exclusive, time.min),
            )

        first_month_delta = values[0] - first_month_base - first_month_deposits
        delta_points.append((months[0], first_month_delta))

        for idx in range(1, len(months)):
            month_start = months[idx]
            month_deposits = deposits_by_month.get(month_start, 0.0)
            month_delta = values[idx] - values[idx - 1] - month_deposits
            delta_points.append((month_start, month_delta))

    if not delta_points:
        return None

    month_labels = [month for month, _ in delta_points]
    deltas = [delta for _, delta in delta_points]
    x = list(range(len(month_labels)))

    title_prefix = f"{year} YTD" if is_ytd else str(year)
    best_idx = max(range(len(deltas)), key=lambda idx: deltas[idx])
    worst_idx = min(range(len(deltas)), key=lambda idx: deltas[idx])
    subtitle = (
        f"Без пополнений. Лучший месяц: {format_month_short_label(month_labels[best_idx])} "
        f"{fmt_compact_rub(deltas[best_idx], signed=True)}, "
        f"худший: {format_month_short_label(month_labels[worst_idx])} "
        f"{fmt_compact_rub(deltas[worst_idx], signed=True)}."
    )

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(fig, f"{title_prefix}: результат по месяцам", subtitle)
    apply_chart_style(ax, rub_axis_formatter)

    bar_colors = []
    for delta in deltas:
        if delta > 0:
            bar_colors.append(CHART_COLORS["positive"])
        elif delta < 0:
            bar_colors.append(CHART_COLORS["negative"])
        else:
            bar_colors.append(CHART_COLORS["neutral"])

    ax.bar(x, deltas, width=0.62, color=bar_colors, edgecolor="none", zorder=3)
    ax.axhline(0, linewidth=1, color=CHART_COLORS["spine"], zorder=2)
    ax.set_ylabel("Результат")
    ax.set_xticks(x)
    ax.set_xticklabels(build_month_tick_labels(month_labels))
    ax.margins(x=0.04, y=0.18)
    annotate_bar_values(
        ax,
        x,
        deltas,
        lambda value: fmt_compact_rub(value, signed=True),
    )

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path












def render_twr_chart(path: str, dates: list[date], values: list[float | None], twr: list[float]) -> str:
    twr_pct = [x * 100.0 for x in twr]

    value_points = [(dt, value) for dt, value in zip(dates, values) if value is not None]
    value_dates = [dt for dt, _ in value_points]
    value_series = [float(value) for _, value in value_points]

    fig, (ax_value, ax_twr) = plt.subplots(
        2,
        1,
        figsize=(10.5, 5.9),
        sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.45]},
    )
    set_chart_header(
        fig,
        ACCOUNT_FRIENDLY_NAME + ": динамика и TWR",
        "Нижний график показывает доходность без искажения пополнениями.",
    )

    apply_chart_style(ax_value, rub_axis_formatter)
    apply_chart_style(ax_twr, pct_axis_formatter)

    if value_series:
        ax_value.fill_between(value_dates, value_series, color=CHART_COLORS["portfolio_fill"], alpha=0.65, zorder=1)
        ax_value.plot(
            value_dates,
            value_series,
            color=CHART_COLORS["portfolio"],
            linewidth=2.6,
            zorder=3,
        )
        ax_value.set_ylabel("Портфель")
        ax_value.margins(x=0.03, y=0.16)
        annotate_series_last_point(
            ax_value,
            value_dates,
            value_series,
            f"{fmt_compact_rub(value_series[-1])}",
            CHART_COLORS["portfolio"],
            y_offset=12,
        )

    positive_twr = [value >= 0 for value in twr_pct]
    negative_twr = [value < 0 for value in twr_pct]
    ax_twr.fill_between(
        dates,
        twr_pct,
        0,
        where=positive_twr,
        color=CHART_COLORS["positive_fill"],
        alpha=0.8,
        interpolate=True,
        zorder=1,
    )
    ax_twr.fill_between(
        dates,
        twr_pct,
        0,
        where=negative_twr,
        color=CHART_COLORS["negative_fill"],
        alpha=0.8,
        interpolate=True,
        zorder=1,
    )
    ax_twr.plot(
        dates,
        twr_pct,
        color=CHART_COLORS["twr"],
        linewidth=2.2,
        zorder=3,
    )
    ax_twr.axhline(0, linewidth=1, color=CHART_COLORS["spine"], zorder=2)
    ax_twr.set_ylabel("TWR")
    ax_twr.margins(x=0.03, y=0.18)
    annotate_series_last_point(
        ax_twr,
        dates,
        twr_pct,
        f"TWR {fmt_compact_pct(twr_pct[-1], signed=True)}",
        CHART_COLORS["twr"],
        y_offset=12 if twr_pct[-1] >= 0 else -12,
    )

    tick_dates, tick_labels = build_date_ticks(dates, max_ticks=7)
    ax_twr.set_xticks(tick_dates)
    ax_twr.set_xticklabels(tick_labels)

    fig.tight_layout(rect=(0, 0, 1, 0.9), h_pad=1.15)
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_dataset_export(session) -> tuple[dict, list[dict], list[dict], list[dict], list[dict]]:
    account_id = resolve_reporting_account_id(session)
    if account_id is None:
        raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    bounds = get_dataset_bounds(session, account_id)
    min_date = bounds["min_date"]
    max_date = bounds["max_date"]
    if min_date is None or max_date is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    daily_rows = get_daily_snapshot_rows(session, account_id)
    positions_rows = list(get_positions_for_snapshot(session, latest_snapshot["id"]))
    asset_alias_rows = list(get_asset_alias_rows(session))
    asset_alias_by_instrument_uid, asset_alias_by_figi = build_asset_alias_lookup(asset_alias_rows)
    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=account_id,
            start_dt=datetime.combine(min_date, time.min),
            end_dt=datetime.combine(max_date + timedelta(days=1), time.min),
        )
    )
    income_rows = list(get_income_events_for_period(session, account_id, min_date, max_date))
    twr_data = compute_twr_timeseries(session, account_id)

    twr_by_date: dict[date, float] = {}
    if twr_data is not None:
        dates, _values, twr_series = twr_data
        twr_by_date = {dt: round(value * 100.0, 6) for dt, value in zip(dates, twr_series)}

    deposits_by_day: dict[date, Decimal] = {}
    withdrawals_by_day: dict[date, Decimal] = {}
    commissions_by_day: dict[date, Decimal] = {}
    taxes_by_day: dict[date, Decimal] = {}
    operations_csv_rows: list[dict] = []
    unknown_operation_groups = 0
    mojibake_detected_count = 0

    for row in operations_rows:
        dt = row["date"]
        local_date = to_local_market_date(dt)
        group = classify_operation_group(row["operation_type"])
        if group == "other":
            unknown_operation_groups += 1

        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        description = row["description"]
        description_has_mojibake = has_mojibake(description)
        if description_has_mojibake:
            mojibake_detected_count += 1

        amount = normalize_decimal(row["amount"])
        amount_abs = abs(amount)
        if local_date is not None:
            if group == "deposit":
                deposits_by_day[local_date] = deposits_by_day.get(local_date, Decimal("0")) + amount
            elif group == "withdrawal":
                withdrawals_by_day[local_date] = withdrawals_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "commission":
                commissions_by_day[local_date] = commissions_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "income_tax" and not is_income_event_backed_tax_operation(row["operation_type"]):
                taxes_by_day[local_date] = taxes_by_day.get(local_date, Decimal("0")) + amount_abs

        operations_csv_rows.append(
            {
                "operation_id": row["operation_id"],
                "date_utc": to_iso_datetime(dt),
                "local_date": local_date.isoformat() if local_date is not None else None,
                "operation_type": row["operation_type"],
                "operation_group": group,
                "state": row["state"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "figi": figi,
                "name": row["name"],
                "amount": amount,
                "currency": row["currency"],
                "price": row["price"],
                "quantity": row["quantity"],
                "commission": row["commission"],
                "yield_amount": row["yield"],
                "description": description,
                "description_has_mojibake": description_has_mojibake,
                "source": row["source"],
            }
        )

    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}
    income_csv_rows: list[dict] = []
    for row in income_rows:
        event_date = row["event_date"]
        alias_row = asset_alias_by_figi.get(row["figi"]) if row.get("figi") else None
        asset_uid = alias_row.get("asset_uid") if alias_row is not None else None
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=alias_row.get("instrument_uid") if alias_row is not None else None,
            figi=row.get("figi"),
        )
        net_amount = normalize_decimal(row["net_amount"])
        tax_amount = normalize_decimal(row["tax_amount"])
        income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
        income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)
        income_csv_rows.append(
            {
                "event_date": event_date,
                "event_type": row["event_type"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "figi": row["figi"],
                "ticker": row["ticker"],
                "instrument_name": row["instrument_name"],
                "gross_amount": row["gross_amount"],
                "tax_amount": row["tax_amount"],
                "net_amount": row["net_amount"],
                "net_yield_pct": row["net_yield_pct"],
                "notified": row["notified"],
            }
        )

    daily_csv_rows: list[dict] = []
    previous_value: Decimal | None = None
    for row in daily_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row["total_value"])
        deposits = deposits_by_day.get(snapshot_date, Decimal("0"))
        withdrawals = withdrawals_by_day.get(snapshot_date, Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        commissions = commissions_by_day.get(snapshot_date, Decimal("0"))
        taxes = taxes_by_day.get(snapshot_date, Decimal("0"))
        income_tax = income_tax_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - taxes
        day_pnl = Decimal("0")
        if previous_value is not None:
            day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

        daily_csv_rows.append(
            {
                "date": snapshot_date,
                "snapshot_at_utc": to_iso_datetime(row["snapshot_at"]),
                "portfolio_value": portfolio_value,
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "deposits": deposits,
                "withdrawals": withdrawals,
                "income_net": income_net,
                "commissions": commissions,
                "operation_taxes": taxes,
                "income_taxes": income_tax,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": twr_by_date.get(snapshot_date),
            }
        )

    positions_csv_rows: list[dict] = []
    for row in positions_rows:
        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        positions_csv_rows.append(
            {
                "snapshot_date": latest_snapshot["snapshot_date"],
                "snapshot_at_utc": to_iso_datetime(latest_snapshot["snapshot_at"]),
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "position_uid": row["position_uid"],
                "figi": figi,
                "ticker": row["ticker"],
                "name": row["name"],
                "instrument_type": row["instrument_type"],
                "quantity": row["quantity"],
                "currency": row["currency"],
                "current_price": row["current_price"],
                "current_nkd": row["current_nkd"],
                "position_value": row["position_value"],
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "weight_pct": row["weight_pct"],
                "value_source": "quantity_x_current_price",
            }
        )

    deposits_total = sum((row["deposits"] for row in daily_csv_rows), Decimal("0"))
    withdrawals_total = sum((row["withdrawals"] for row in daily_csv_rows), Decimal("0"))
    income_net_total = sum((row["income_net"] for row in daily_csv_rows), Decimal("0"))
    commissions_total = sum((row["commissions"] for row in daily_csv_rows), Decimal("0"))
    operation_taxes_total = sum((row["operation_taxes"] for row in daily_csv_rows), Decimal("0"))
    income_taxes_total = sum((row["income_taxes"] for row in daily_csv_rows), Decimal("0"))
    current_value = normalize_decimal(latest_snapshot["total_value"])
    net_contributions = deposits_total - withdrawals_total
    period_start_value = normalize_decimal(daily_csv_rows[0]["portfolio_value"])
    period_end_value = current_value
    period_net_cashflow = sum((row["net_cashflow"] for row in daily_csv_rows[1:]), Decimal("0"))
    period_pnl_abs = period_end_value - period_start_value - period_net_cashflow
    has_full_history_from_zero = period_start_value == Decimal("0")
    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        latest_snapshot,
        positions_rows,
    )
    alias_groups_count = len({row["asset_uid"] for row in asset_alias_rows if row.get("asset_uid")})

    positions_missing_labels = sum(1 for row in positions_csv_rows if not (row["ticker"] or row["name"]))

    dataset = {
        "meta": {
            "dataset_version": 2,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "timezone": TZ_NAME,
            "account_name": ACCOUNT_FRIENDLY_NAME,
            "period_start": min_date.isoformat(),
            "period_end": max_date.isoformat(),
            "base_currency": latest_snapshot["currency"],
            "latest_snapshot_at": to_iso_datetime(latest_snapshot["snapshot_at"]),
        },
        "summary": {
            "current_value": current_value,
            "net_contributions": net_contributions,
            "deposits_total": deposits_total,
            "withdrawals_total": withdrawals_total,
            "income_net_total": income_net_total,
            "commissions_total": commissions_total,
            "income_taxes_total": income_taxes_total,
            "operation_taxes_total": operation_taxes_total,
            "taxes_total": income_taxes_total + operation_taxes_total,
            "period_start_value": period_start_value,
            "period_end_value": period_end_value,
            "period_net_cashflow": period_net_cashflow,
            "period_pnl_abs": period_pnl_abs,
            "period_twr_pct": twr_by_date.get(max_date),
            "has_full_history_from_zero": has_full_history_from_zero,
            "positions_value_sum": positions_value_sum,
            "reconciliation_gap_abs": reconciliation_gap_abs,
            "reconciliation_by_asset_type": reconciliation_rows,
            "snapshot_count": len(daily_csv_rows),
            "positions_count": len(positions_csv_rows),
            "operations_count": len(operations_csv_rows),
            "income_events_count": len(income_csv_rows),
        },
        "timeseries_daily": daily_csv_rows,
        "positions_current": positions_csv_rows,
        "operations": operations_csv_rows,
        "income_events": income_csv_rows,
        "asset_aliases": [
            {
                "logical_asset_id": row["asset_uid"],
                "asset_uid": row["asset_uid"],
                "instrument_uid": row["instrument_uid"],
                "figi": row["figi"],
                "ticker": row["ticker"],
                "name": row["name"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in asset_alias_rows
        ],
        "data_quality": {
            "unknown_operation_group_count": unknown_operation_groups,
            "mojibake_detected_count": mojibake_detected_count,
            "positions_missing_label_count": positions_missing_labels,
            "has_full_history_from_zero": has_full_history_from_zero,
            "alias_groups_count": alias_groups_count,
            "income_events_available": True,
        },
        "assumptions": [
            "В operations включены только исполненные операции после дедупликации по operation_id.",
            "Дневные cashflow-агрегаты привязаны к локальной дате Europe/Moscow.",
            "income_net в daily timeseries уже учитывает удержанный налог из income_events.",
            "operation_taxes_total не включает dividend/coupon tax, если тот же налог уже представлен в income_events.",
            "Архив считается period-first: lifetime return не вычисляется без полной истории с нуля.",
            "reconciliation_by_asset_type строится от snapshot totals по классам активов; нераскрытый остаток остаётся residual.",
        ],
    }

    return dataset, daily_csv_rows, positions_csv_rows, operations_csv_rows, income_csv_rows


def build_dataset_readme(dataset: dict) -> str:
    summary = dataset["summary"]
    meta = dataset["meta"]
    return (
        "# FinanceTracker AI Dataset\n\n"
        "Этот архив подготовлен командой `/dataset` для передачи ИИ-модели.\n\n"
        "## Контекст\n\n"
        f"- Счёт: {meta['account_name']}\n"
        f"- Таймзона агрегации: {meta['timezone']}\n"
        f"- Период: {meta['period_start']} .. {meta['period_end']}\n"
        f"- Базовая валюта: {meta['base_currency']}\n"
        f"- Сформировано: {meta['generated_at']}\n\n"
        "## Файлы\n\n"
        "- `dataset.json` — основной структурированный датасет для ИИ.\n"
        "- `daily_timeseries.csv` — дневной ряд стоимости и денежных потоков.\n"
        "- `positions_current.csv` — текущие позиции на последнем снапшоте.\n"
        "- `operations.csv` — исполненные операции после дедупликации.\n"
        "- `income_events.csv` — купоны и дивиденды в нормализованном виде.\n\n"
        "## Ключевые поля\n\n"
        f"- Current value: {decimal_to_str(summary['current_value'])} {meta['base_currency']}\n"
        f"- Period start value: {decimal_to_str(summary['period_start_value'])} {meta['base_currency']}\n"
        f"- Period end value: {decimal_to_str(summary['period_end_value'])} {meta['base_currency']}\n"
        f"- Period net cashflow: {decimal_to_str(summary['period_net_cashflow'])} {meta['base_currency']}\n"
        f"- Period pnl abs: {decimal_to_str(summary['period_pnl_abs'])} {meta['base_currency']}\n"
        f"- Period twr pct: {summary['period_twr_pct']}\n"
        f"- Positions value sum: {decimal_to_str(summary['positions_value_sum'])} {meta['base_currency']}\n"
        f"- Reconciliation gap abs: {decimal_to_str(summary['reconciliation_gap_abs'])} {meta['base_currency']}\n"
        f"- Full history from zero: {summary['has_full_history_from_zero']}\n\n"
        "## Правила интерпретации\n\n"
        "- Денежные значения в JSON сохраняются как строки, чтобы не терять точность.\n"
        "- `operation_group` нормализует сырые типы операций; налоги по операциям экспортируются как `income_tax`.\n"
        "- `logical_asset_id` строится из `asset_uid` и нужен для склейки бумаг при смене FIGI.\n"
        "- `income_net` в дневном ряду уже очищен от удержанного налога по income_events.\n"
        "- `taxes_total` дедуплицирован: dividend/coupon tax не суммируется второй раз из operations, если он уже попал в income_events.\n"
        "- Если `has_full_history_from_zero=false`, архив нельзя трактовать как полную lifetime-историю портфеля.\n"
        "- Если `reconciliation_gap_abs` не равен нулю, смотрите `reconciliation_by_asset_type`: это residual между snapshot totals и суммой позиционных оценок.\n"
        "- Для подробного анализа сначала читайте `dataset.json`, затем CSV-файлы как табличную детализацию.\n"
    )


def create_dataset_archive() -> tuple[str, str]:
    with db_session() as session:
        dataset, daily_rows, positions_rows, operations_rows, income_rows = build_dataset_export(session)

    archive_name = f"fintracker_dataset_{dataset['meta']['period_end']}.zip"
    archive_tmp = tempfile.NamedTemporaryFile(prefix="fintracker_dataset_", suffix=".zip", delete=False)
    archive_path = archive_tmp.name
    archive_tmp.close()

    json_text = json.dumps(dataset, ensure_ascii=False, indent=2, default=json_default)
    readme_text = build_dataset_readme(dataset)

    daily_fields = [
        "date",
        "snapshot_at_utc",
        "portfolio_value",
        "expected_yield",
        "expected_yield_pct",
        "deposits",
        "withdrawals",
        "income_net",
        "commissions",
        "operation_taxes",
        "income_taxes",
        "net_cashflow",
        "day_pnl",
        "twr_pct",
    ]
    positions_fields = [
        "snapshot_date",
        "snapshot_at_utc",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "position_uid",
        "figi",
        "ticker",
        "name",
        "instrument_type",
        "quantity",
        "currency",
        "current_price",
        "current_nkd",
        "position_value",
        "expected_yield",
        "expected_yield_pct",
        "weight_pct",
        "value_source",
    ]
    operations_fields = [
        "operation_id",
        "date_utc",
        "local_date",
        "operation_type",
        "operation_group",
        "state",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "figi",
        "name",
        "amount",
        "currency",
        "price",
        "quantity",
        "commission",
        "yield_amount",
        "description",
        "description_has_mojibake",
        "source",
    ]
    income_fields = [
        "event_date",
        "event_type",
        "logical_asset_id",
        "asset_uid",
        "figi",
        "ticker",
        "instrument_name",
        "gross_amount",
        "tax_amount",
        "net_amount",
        "net_yield_pct",
        "notified",
    ]

    with tempfile.TemporaryDirectory(prefix="fintracker_dataset_") as temp_dir:
        dataset_json_path = os.path.join(temp_dir, "dataset.json")
        with open(dataset_json_path, "w", encoding="utf-8") as f:
            f.write(json_text)

        readme_path = os.path.join(temp_dir, "README_AI.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_text)

        write_csv_file(os.path.join(temp_dir, "daily_timeseries.csv"), daily_fields, daily_rows)
        write_csv_file(os.path.join(temp_dir, "positions_current.csv"), positions_fields, positions_rows)
        write_csv_file(os.path.join(temp_dir, "operations.csv"), operations_fields, operations_rows)
        write_csv_file(os.path.join(temp_dir, "income_events.csv"), income_fields, income_rows)

        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(dataset_json_path, arcname="dataset.json")
            archive.write(readme_path, arcname="README_AI.md")
            archive.write(os.path.join(temp_dir, "daily_timeseries.csv"), arcname="daily_timeseries.csv")
            archive.write(os.path.join(temp_dir, "positions_current.csv"), arcname="positions_current.csv")
            archive.write(os.path.join(temp_dir, "operations.csv"), arcname="operations.csv")
            archive.write(os.path.join(temp_dir, "income_events.csv"), arcname="income_events.csv")

    return archive_path, archive_name




async def jobqueue_smoke_test_job(context: ContextTypes.DEFAULT_TYPE):
    sent = 0
    failed = 0
    now_local = datetime.now(TZ)
    text_msg = (
        "🧪 JobQueue smoke-test\n"
        f"Время (локальное): {now_local.strftime('%d.%m.%Y %H:%M:%S %Z')}\n"
        "Отправка из одноразового тестового джоба при старте."
    )

    for chat_id in TARGET_CHAT_IDS:
        try:
            await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
            logger.exception(
                "jobqueue_smoke_test_failed",
                "JobQueue smoke-test failed.",
                {"chat_id": chat_id},
            )

    logger.info(
        "jobqueue_smoke_test_completed",
        "JobQueue smoke-test completed.",
        {
            "sent": sent,
            "failed": failed,
            "target_chat_ids": sorted(TARGET_CHAT_IDS),
        },
    )


# =============== HANDLERS =================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/start")
    if not is_authorized(update):
        return
    text = (
        "Привет! Я слежу за вашим портфелем «Семейный капитал».\n\n"
        "Доступные команды можно посмотреть в /help."
    )
    logger.info(
        "bot_reply_text_started",
        "Sending reply_text response.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )
    await update.message.reply_text(text)
    logger.info(
        "bot_reply_text_succeeded",
        "reply_text response sent.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/help")
    if not is_authorized(update):
        return

    text = build_help_text()
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/today")
    if not is_authorized(update):
        return
    text = build_today_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/week")
    if not is_authorized(update):
        return
    text = build_week_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/month")
    if not is_authorized(update):
        return
    text = build_month_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/year")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) > 1:
        await update.message.reply_text("Формат: /year или /year YYYY")
        return

    year: int | None = None
    if len(args) == 1:
        try:
            parsed_year = int(args[0])
            if parsed_year < 1900 or parsed_year > 2100:
                raise ValueError
            year = parsed_year
        except ValueError:
            await update.message.reply_text("Формат: /year или /year YYYY")
            return

    try:
        summary_text, diff_text, label = build_year_summary(year)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    temp_chart = tempfile.NamedTemporaryFile(prefix=f"year_{chart_year}_", suffix=".png", delete=False)
    chart_path = temp_chart.name
    temp_chart.close()
    try:
        chart = build_year_chart(
            chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(chart_path):
            os.remove(chart_path)
        await update.message.reply_text(str(exc))
        return
    if chart:
        try:
            with open(chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(chart):
                os.remove(chart)
    else:
        await update.message.reply_text(f"Недостаточно данных для графика за {label}.")

    temp_delta_chart = tempfile.NamedTemporaryFile(prefix=f"year_delta_{chart_year}_", suffix=".png", delete=False)
    delta_chart_path = temp_delta_chart.name
    temp_delta_chart.close()
    try:
        delta_chart = build_year_monthly_delta_chart(
            delta_chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(delta_chart_path):
            os.remove(delta_chart_path)
        await update.message.reply_text(str(exc))
        return
    if delta_chart:
        try:
            with open(delta_chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(delta_chart):
                os.remove(delta_chart)

    await safe_send_message(context.bot, update.effective_chat.id, diff_text, parse_mode="Markdown")


async def cmd_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/dataset")
    if not is_authorized(update):
        return

    if context.args:
        await update.message.reply_text("Формат: /dataset")
        return

    try:
        archive_path, archive_name = create_dataset_archive()
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    try:
        with open(archive_path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=archive_name),
                caption="Архив для AI-анализа: JSON, CSV и README с контекстом.",
            )
    finally:
        if os.path.exists(archive_path):
            os.remove(archive_path)


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/structure")
    if not is_authorized(update):
        return
    text = build_structure_text()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/history")
    if not is_authorized(update):
        return

    path = "/tmp/history.png"
    try:
        p = build_history_chart(path)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    if not p:
        await update.message.reply_text(
            "Недостаточно данных для построения графика."
        )
        return

    with open(p, "rb") as f:
        # Caption убран по требованию
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_twr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/twr")
    if not is_authorized(update):
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return

        data = compute_twr_timeseries(session, account_id)
        xirr_value, projected_value, projection_date = compute_portfolio_xirr_and_run_rate(
            session,
            account_id,
        )

    if not data:
        await update.message.reply_text("Недостаточно данных")
        return

    dates, values, twr = data
    last_date = dates[-1]
    last_value = values[-1]
    last_twr_pct = twr[-1] * 100.0
    summary_text = render_twr_summary_text(
        last_date=last_date,
        last_value=last_value,
        last_twr_pct=last_twr_pct,
        xirr_value=xirr_value,
        projected_value=projected_value,
        projection_date=projection_date,
    )
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    path = "/tmp/twr.png"
    render_twr_chart(path, dates, values, twr)

    with open(path, "rb") as f:
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/targets")
    if not is_authorized(update):
        return

    args = context.args or []
    if not args:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if account_id is None:
                await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
                return
            text = build_targets_text_for_account(session, account_id)
        await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
        return

    if args[0].lower() != "set":
        await update.message.reply_text(TARGETS_USAGE_TEXT)
        return

    try:
        targets = parse_rebalance_targets_args(args[1:])
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        saved = replace_rebalance_targets(session, account_id, targets)
        if not saved:
            await update.message.reply_text(REBALANCE_FEATURE_UNAVAILABLE_TEXT)
            return

    with db_session() as session:
        text = build_targets_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/rebalance")
    if not is_authorized(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /rebalance")
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_rebalance_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_invest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/invest")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) != 1:
        await update.message.reply_text(INVEST_USAGE_TEXT)
        return

    try:
        amount = parse_decimal_input(args[0], allow_zero=False)
        if quantize_ruble_amount(amount) <= 0:
            raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    rounded_amount = quantize_ruble_amount(amount)
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_invest_text_for_account(
            session,
            account_id,
            rounded_amount,
            header=f"💸 Как распределить пополнение {fmt_decimal_rub(rounded_amount, precision=0)}",
        )
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


# ============ DAILY JOB (JOBQUEUE) ========


async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Авто-рассылки по расписанию (по времени хоста):
    - каждый день в 18:00 (по времени хоста): проверка триггеров (новый максимум / годовой план)
    - каждую пятницу в 18:00 (по времени хоста): недельный отчёт (/week)
    - в последний день месяца в 18:00 (по времени хоста): месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = f"18:00 {HOST_TZ}"

    logger.info(
        "daily_job_started",
        "Daily job started.",
        {
            "today": today.isoformat(),
            "scheduled_for": scheduled_for,
            "started_at": started_at.isoformat(),
            "is_month_end": is_month_end,
            "is_friday": is_friday,
        },
    )

    month_text = None
    week_text = None
    triggers: list[str] = []

    try:
        if is_month_end:
            month_text = build_month_summary()
    except Exception:
        logger.exception("daily_job_month_summary_failed", "Failed to build month summary.")

    try:
        if is_friday:
            week_text = build_week_summary()
    except Exception:
        logger.exception("daily_job_week_summary_failed", "Failed to build week summary.")

    try:
        triggers = build_triggers_messages()
    except Exception:
        logger.exception("daily_job_triggers_failed", "Failed to build trigger messages.")

    logger.info(
        "daily_job_messages_prepared",
        "Daily job prepared messages.",
        {
            "month_report_ready": bool(month_text),
            "week_report_ready": bool(week_text),
            "triggers_count": len(triggers),
        },
    )

    # Нечего отправлять — выходим тихо.
    if not month_text and not week_text and not triggers:
        logger.info(
            "daily_job_no_messages",
            "Daily job had no messages to send.",
            {"today": today.isoformat()},
        )
        return

    sent_total = 0
    failed_total = 0

    for chat_id in TARGET_CHAT_IDS:
        # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
        if is_month_end and month_text:
            try:
                await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job month report.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )

        if is_friday and week_text:
            try:
                await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job week report.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )

        for msg in triggers:
            try:
                await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job trigger message.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    logger.info(
        "daily_job_completed",
        "Daily job completed.",
        {
            "today": today.isoformat(),
            "duration_ms": duration_ms,
            "sent_total": sent_total,
            "failed_total": failed_total,
        },
    )


async def check_income_events(context: ContextTypes.DEFAULT_TYPE):
    rows: list[dict] = []
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return

        rows = get_unnotified_income_events(session, account_id)

    for row in rows:
        event_type = row["event_type"]
        icon = "💸" if event_type == "coupon" else "💰"
        action_line = "Купон зачислен" if event_type == "coupon" else "Дивиденды зачислены"
        net_amount = float(row["net_amount"])
        net_yield_pct = float(row["net_yield_pct"])
        instrument_name = row["instrument_name"]

        text_msg = (
            f"{icon} {instrument_name}\n"
            f"{action_line}\n"
            f"{fmt_signed_amount(net_amount)} ₽ ({fmt_plain_pct(net_yield_pct)} %)"
        )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "income_event_notification_sent",
                    "Income event notification sent.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "income_event_notification_failed",
                    "Failed to send income event notification.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            mark_income_event_notified(session, row["id"])


async def check_invest_notifications(context: ContextTypes.DEFAULT_TYPE):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return
        rows = get_pending_invest_notifications(session, account_id)

    if rows is None:
        return

    for row in rows:
        amount = normalize_decimal(row["amount"])
        with db_session() as session:
            text_msg = build_invest_text_for_account(
                session,
                account_id,
                amount,
                header=f"💸 Получено пополнение: {fmt_decimal_rub(amount, precision=0)}",
            )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "invest_notification_sent",
                    "Invest notification sent.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "invest_notification_failed",
                    "Failed to send invest notification.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            marked = mark_invest_notification_sent(
                session,
                account_id=account_id,
                operation_id=row["operation_id"],
                operation_date=row["date"],
                amount=amount,
            )
        if not marked:
            return


def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "missing_telegram_bot_token",
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную.",
        )
        return 1

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.COMMAND, debug_command_probe), group=-1)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("month", cmd_month))
    app.add_handler(CommandHandler("year", cmd_year))
    app.add_handler(CommandHandler("dataset", cmd_dataset))
    app.add_handler(CommandHandler("structure", cmd_structure))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("twr", cmd_twr))
    app.add_handler(CommandHandler("targets", cmd_targets))
    app.add_handler(CommandHandler("rebalance", cmd_rebalance))
    app.add_handler(CommandHandler("invest", cmd_invest))

    # Ежедневный джоб
    job_time = time(
        hour=18,
        minute=0,
        tzinfo=HOST_TZ,
    )
    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    app.job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    app.job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")
    app.job_queue.run_repeating(check_invest_notifications, interval=60, first=15, name="invest_notifier")

    if JOBQUEUE_SMOKE_TEST_ON_START:
        app.job_queue.run_once(
            jobqueue_smoke_test_job,
            when=JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
            name="jobqueue_smoke_test",
        )
        logger.info(
            "bot_jobqueue_smoke_scheduled",
            "Scheduled one-time JobQueue smoke-test.",
            {
                "delay_seconds": JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
                "target_chat_ids": sorted(TARGET_CHAT_IDS),
            },
        )

    logger.info(
        "bot_started",
        "Bot started.",
        {
            "daily_job_time_local": "18:00",
            "host_timezone": str(HOST_TZ),
        },
    )
    app.run_polling()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "bot_process_failed",
            "Bot process terminated with an unhandled exception.",
        )
        raise SystemExit(1)
