"""Monthly report artifact orchestration."""

from __future__ import annotations

from typing import Any, Callable

from financetracker.reporting.narrative import build_deterministic_monthly_narrative
from financetracker.reporting.report_charts import build_monthly_report_charts


def build_monthly_report_filename(payload: dict[str, Any]) -> str:
    return f"fintracker_monthly_{payload['meta']['period_year']}-{payload['meta']['period_month']:02d}.pdf"


def build_monthly_report_artifact(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
    html_builder: Callable[..., str],
    pdf_builder: Callable[..., bytes],
    charts_builder: Callable[[dict[str, Any]], dict[str, str | None]] = build_monthly_report_charts,
    narrative_builder: Callable[[dict[str, Any]], dict[str, Any]] = build_deterministic_monthly_narrative,
    filename_builder: Callable[[dict[str, Any]], str] = build_monthly_report_filename,
) -> dict[str, Any]:
    charts = charts_builder(payload)
    resolved_narrative = narrative or narrative_builder(payload)
    html = html_builder(payload, narrative=resolved_narrative, charts=charts)
    pdf_bytes = pdf_builder(payload, narrative=resolved_narrative, charts=charts, html=html, pdf_renderer=pdf_renderer)
    return {"schema_version": "monthly_report_artifact.v1", "payload": payload, "narrative": resolved_narrative, "charts": charts, "html": html, "pdf_bytes": pdf_bytes, "filename": filename_builder(payload)}


def create_monthly_report_artifact(
    *,
    year: int | None = None,
    month: int | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
    payload_builder: Callable[..., dict[str, Any]],
    artifact_builder: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    payload = payload_builder(year=year, month=month)
    return artifact_builder(payload, pdf_renderer=pdf_renderer)
