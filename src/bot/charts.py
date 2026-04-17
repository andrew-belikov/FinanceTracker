from __future__ import annotations

from datetime import date, datetime, time, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

from queries import (
    get_deposits_by_date,
    get_deposits_sum_for_period,
    get_first_snapshot_in_period,
    get_last_snapshot_before_date,
    get_monthly_deposits,
    get_monthly_portfolio_values,
    get_portfolio_timeseries,
    resolve_reporting_account_id,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    SHORT_MONTHS_RU,
    TZ,
    db_session,
    fmt_compact_pct,
    fmt_compact_rub,
    fmt_rub,
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
    "peak": "#d64545",
    "neutral": "#8f97a6",
    "grid": "#d8dfe7",
    "spine": "#d6dce3",
    "text": "#1f2933",
    "muted": "#67707b",
}


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


def annotate_point(
    ax,
    x_value,
    y_value: float,
    label: str,
    color: str,
    *,
    x_offset: int,
    y_offset: int,
    marker_size: int = 34,
    marker_edge_color: str = "white",
    bbox_edge_color: str | None = None,
    show_arrow: bool = False,
):
    ax.scatter(
        [x_value],
        [y_value],
        color=color,
        s=marker_size,
        edgecolors=marker_edge_color,
        linewidths=0.9,
        zorder=5,
    )
    arrowprops = None
    if show_arrow:
        arrowprops = {
            "arrowstyle": "-",
            "color": color,
            "lw": 1.0,
            "shrinkA": 6,
            "shrinkB": 6,
            "alpha": 0.85,
        }
    ax.annotate(
        label,
        xy=(x_value, y_value),
        xytext=(x_offset, y_offset),
        textcoords="offset points",
        ha="left" if x_offset >= 0 else "right",
        va="center",
        fontsize=9,
        color=CHART_COLORS["text"],
        bbox={
            "boxstyle": "round,pad=0.3",
            "fc": "white",
            "ec": bbox_edge_color or color,
            "lw": 0.9,
            "alpha": 0.97,
        },
        arrowprops=arrowprops,
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

    peak_value = max(values)
    peak_index = max(idx for idx, value in enumerate(values) if value == peak_value)
    peak_date = week_dates[peak_index]
    value_span = peak_value - min(values)
    peak_x_offset = -16 if peak_index >= len(week_dates) // 2 else 16
    peak_y_offset = -18 if value_span > 0 and peak_value >= (min(values) + value_span * 0.7) else 18
    peak_label = (
        f"{format_day_month_label(peak_date, include_year=True).replace(chr(10), ' ')}\n"
        f"{fmt_rub(peak_value)}"
    )
    annotate_point(
        ax,
        peak_date,
        peak_value,
        peak_label,
        CHART_COLORS["peak"],
        x_offset=peak_x_offset,
        y_offset=peak_y_offset,
        marker_size=42,
        bbox_edge_color=CHART_COLORS["spine"],
        show_arrow=True,
    )

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
