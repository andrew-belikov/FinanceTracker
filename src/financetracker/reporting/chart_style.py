"""Renderer-local Matplotlib styling; independent from bot and database."""

from datetime import date

from matplotlib.ticker import FuncFormatter, MaxNLocator


CHART_COLORS = {
    "portfolio": "#1f6f8b", "portfolio_fill": "#d9eef4", "deposits": "#d8a25e",
    "deposits_fill": "#f6e4c9", "deductions": "#8a6fb0", "twr": "#6b7aa1",
    "positive": "#3e8e63", "positive_fill": "#dcefe3", "negative": "#c46b4f",
    "negative_fill": "#f5dfd7", "peak": "#d64545", "neutral": "#8f97a6",
    "text": "#1f2933", "muted": "#67707b", "grid": "#d8dfe7", "spine": "#d6dce3",
}


def build_date_ticks(dates: list[date], max_ticks: int = 7) -> tuple[list[date], list[str]]:
    if not dates:
        return [], []
    indices = list(range(len(dates))) if len(dates) <= max_ticks else [round(i * (len(dates) - 1) / (max_ticks - 1)) for i in range(max_ticks)]
    selected = [dates[index] for index in dict.fromkeys(indices)]
    previous_year = None
    labels = []
    for value in selected:
        label = f"{value.day} {value.strftime('%b')}"
        if previous_year is None or previous_year != value.year:
            label += f"\n{value.year}"
        labels.append(label)
        previous_year = value.year
    return selected, labels


def rub_axis_formatter(value: float, _pos=None) -> str:
    return f"{value / 1000:,.1f} тыс. ₽".replace(",", " ")


def set_chart_header(fig, title: str, subtitle: str | None = None):
    fig.patch.set_facecolor("white")
    fig.suptitle(title, x=0.125, y=0.972, ha="left", fontsize=14, fontweight="bold", color=CHART_COLORS["text"])
    if subtitle:
        fig.text(0.125, 0.905, subtitle, ha="left", va="top", fontsize=9, color=CHART_COLORS["muted"])


def apply_chart_style(ax, y_formatter=None):
    ax.set_facecolor("white")
    ax.grid(axis="y", color=CHART_COLORS["grid"], linewidth=0.8, alpha=0.7)
    ax.grid(axis="x", visible=False)
    ax.tick_params(axis="both", labelsize=9, colors=CHART_COLORS["muted"])
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    if y_formatter is not None:
        ax.yaxis.set_major_formatter(FuncFormatter(y_formatter))


def annotate_series_last_point(ax, x_values, y_values, label: str, color: str, y_offset: int = 0):
    if not x_values or not y_values:
        return
    ax.scatter([x_values[-1]], [y_values[-1]], color=color, s=28, zorder=5)
    ax.annotate(label, xy=(x_values[-1], y_values[-1]), xytext=(10, y_offset), textcoords="offset points", fontsize=9, color=CHART_COLORS["text"])


def annotate_bar_values(ax, x_values, values, formatter, text_color: str | None = None):
    visible = [abs(value) for value in values if value is not None]
    if not visible:
        return
    offset = max(max(visible) * 0.04, 1.0)
    for x, value in zip(x_values, values):
        if value is not None:
            ax.text(x, value + (offset if value >= 0 else -offset), formatter(value), ha="center", va="bottom" if value >= 0 else "top", color=text_color or CHART_COLORS["text"], fontsize=8)


def set_value_axis_limits(ax, values, min_padding_ratio: float = 0.12, flat_padding_ratio: float = 0.05):
    finite = [value for value in values if value is not None]
    if not finite:
        return
    minimum, maximum = min(finite), max(finite)
    span = maximum - minimum
    padding = max(abs(maximum) * flat_padding_ratio, 1.0) if span <= 0 else max(span * min_padding_ratio, 1.0)
    ax.set_ylim(minimum - padding, maximum + padding)
