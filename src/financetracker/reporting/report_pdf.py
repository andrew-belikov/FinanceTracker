"""PDF and debug-HTML rendering for monthly reports."""

from __future__ import annotations

import os
from typing import Any, Callable

from financetracker.common.logging_setup import get_logger
from financetracker.reporting.debug_artifacts import save_debug_text


REPORT_DEBUG_SAVE_HTML = os.getenv("REPORT_DEBUG_SAVE_HTML", "false").strip().lower() in {"1", "true", "yes", "on"}
logger = get_logger(__name__)


class ReportRenderError(RuntimeError):
    pass


def save_debug_report_html(html: str) -> str:
    return save_debug_text(kind="html", suffix=".html", text=html)


def build_monthly_report_pdf_bytes(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    charts: dict[str, str | None] | None = None,
    html: str | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
    html_builder: Callable[..., str] | None = None,
) -> bytes:
    if html is None:
        if html_builder is None:
            raise ValueError("html_builder обязателен, если HTML не передан.")
        html = html_builder(payload, narrative=narrative, charts=charts)
    if REPORT_DEBUG_SAVE_HTML:
        save_debug_report_html(html)
        logger.info("monthly_report_html_debug_saved", "Saved monthly report HTML to a debug file.", {"artifact_kind": "html"})
    if pdf_renderer is not None:
        return pdf_renderer(html)
    try:
        from weasyprint import HTML
    except Exception as exc:  # pragma: no cover
        raise ReportRenderError("Не удалось импортировать WeasyPrint для PDF-рендера.") from exc
    try:
        pdf_bytes = HTML(string=html).write_pdf()
    except Exception as exc:  # pragma: no cover
        raise ReportRenderError("Не удалось собрать PDF через WeasyPrint.") from exc
    logger.info("monthly_report_pdf_built", "Built deterministic monthly report PDF bytes.", {"period": f"{payload['meta']['period_year']}-{payload['meta']['period_month']:02d}", "size_bytes": len(pdf_bytes)})
    return pdf_bytes
