"""Compatibility facade for monthly report rendering."""

from __future__ import annotations

from typing import Any, Callable

from financetracker.reporting.narrative import (  # noqa: F401
    build_deterministic_monthly_narrative,
)
from financetracker.reporting.report_artifact import (
    build_monthly_report_artifact as _build_monthly_report_artifact,
    build_monthly_report_filename as build_monthly_report_filename,
    create_monthly_report_artifact as _create_monthly_report_artifact,
)
from financetracker.reporting.report_charts import (  # noqa: F401
    build_asset_class_breakdown as _build_asset_class_breakdown,
    build_monthly_report_charts as build_monthly_report_charts,
)
from financetracker.reporting.report_html import build_monthly_report_html
from financetracker.reporting.report_html_parts import (  # noqa: F401
    _build_asset_class_summary_rows as _build_asset_class_summary_rows,
    _build_plan_pace_fact as _build_plan_pace_fact,
    _build_weight_transition_map as _build_weight_transition_map,
    _classify_day_pnl_rows as _classify_day_pnl_rows,
    _compute_average_day_pnl as _compute_average_day_pnl,
)
from financetracker.reporting.report_payload import create_monthly_report_payload
from financetracker.reporting.report_pdf import (
    ReportRenderError as ReportRenderError,
    build_monthly_report_pdf_bytes as _build_monthly_report_pdf_bytes,
    save_debug_report_html as save_debug_report_html,
)


def build_monthly_report_pdf_bytes(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    charts: dict[str, str | None] | None = None,
    html: str | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> bytes:
    return _build_monthly_report_pdf_bytes(
        payload,
        narrative=narrative,
        charts=charts,
        html=html,
        pdf_renderer=pdf_renderer,
        html_builder=build_monthly_report_html,
    )


def build_monthly_report_artifact(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    return _build_monthly_report_artifact(
        payload,
        narrative=narrative,
        pdf_renderer=pdf_renderer,
        html_builder=build_monthly_report_html,
        pdf_builder=build_monthly_report_pdf_bytes,
        charts_builder=build_monthly_report_charts,
        narrative_builder=build_deterministic_monthly_narrative,
        filename_builder=build_monthly_report_filename,
    )


def create_monthly_report_artifact(
    *,
    year: int | None = None,
    month: int | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    return _create_monthly_report_artifact(
        year=year,
        month=month,
        pdf_renderer=pdf_renderer,
        payload_builder=create_monthly_report_payload,
        artifact_builder=build_monthly_report_artifact,
    )
