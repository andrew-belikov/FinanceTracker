from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from common.logging_setup import get_logger
from report_payload import create_monthly_report_payload
from report_render import build_monthly_report_artifact


REPORT_SCHEMA_VERSION = "monthly_report_stub.v1"
REPORT_ARTIFACT_SCHEMA_VERSION = "monthly_report_artifact.v1"
REPORT_PDF_ENGINE = os.getenv("REPORT_PDF_ENGINE", "placeholder").strip() or "placeholder"
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
TZ = ZoneInfo(TZ_NAME)
OLLAMA_ENABLED = os.getenv("OLLAMA_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").strip() or "http://ollama:11434"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b").strip() or "qwen2.5:1.5b"
REPORT_DEBUG_SAVE_HTML = os.getenv("REPORT_DEBUG_SAVE_HTML", "false").strip().lower() in {"1", "true", "yes", "on"}
REPORT_DEBUG_SAVE_PAYLOAD = os.getenv("REPORT_DEBUG_SAVE_PAYLOAD", "false").strip().lower() in {"1", "true", "yes", "on"}

logger = get_logger(__name__)


class ReportRequestError(ValueError):
    pass


def resolve_monthly_report_period(
    *,
    year: int | None = None,
    month: int | None = None,
    now: datetime | None = None,
) -> tuple[int, int]:
    current = now.astimezone(TZ) if now is not None else datetime.now(TZ)
    resolved_year = year if year is not None else current.year
    resolved_month = month if month is not None else current.month

    if resolved_year < 1900 or resolved_year > 2100:
        raise ReportRequestError("Поле year должно быть в диапазоне 1900..2100.")
    if resolved_month < 1 or resolved_month > 12:
        raise ReportRequestError("Поле month должно быть в диапазоне 1..12.")

    return resolved_year, resolved_month


def build_report_health_payload() -> dict:
    return {
        "status": "ok",
        "service": "reporter",
        "schema_version": REPORT_SCHEMA_VERSION,
        "pdf_engine": REPORT_PDF_ENGINE,
        "timezone": TZ_NAME,
        "ollama_enabled": OLLAMA_ENABLED,
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_model": OLLAMA_MODEL,
        "debug_save_html": REPORT_DEBUG_SAVE_HTML,
        "debug_save_payload": REPORT_DEBUG_SAVE_PAYLOAD,
    }


def build_monthly_pdf_stub_response(payload: dict[str, object] | None) -> dict:
    request_payload = payload or {}
    year = request_payload.get("year")
    month = request_payload.get("month")

    if year is not None and not isinstance(year, int):
        raise ReportRequestError("Поле year должно быть целым числом.")
    if month is not None and not isinstance(month, int):
        raise ReportRequestError("Поле month должно быть целым числом.")

    resolved_year, resolved_month = resolve_monthly_report_period(
        year=year,
        month=month,
    )
    period = f"{resolved_year}-{resolved_month:02d}"
    logger.info(
        "report_pipeline_monthly_stub_requested",
        "Monthly PDF stub request resolved.",
        {
            "period": period,
            "request_keys": sorted(request_payload.keys()),
            "pdf_engine": REPORT_PDF_ENGINE,
            "ollama_enabled": OLLAMA_ENABLED,
        },
    )
    return {
        "status": "not_implemented",
        "report_kind": "monthly_pdf",
        "period": period,
        "schema_version": REPORT_SCHEMA_VERSION,
        "message": "Monthly PDF generation is not implemented yet.",
        "request_keys": sorted(request_payload.keys()),
    }


def build_monthly_report_artifact_for_request(
    payload: dict[str, object] | None,
    *,
    pdf_renderer=None,
) -> dict:
    request_payload = payload or {}
    year = request_payload.get("year")
    month = request_payload.get("month")

    if year is not None and not isinstance(year, int):
        raise ReportRequestError("Поле year должно быть целым числом.")
    if month is not None and not isinstance(month, int):
        raise ReportRequestError("Поле month должно быть целым числом.")

    resolved_year, resolved_month = resolve_monthly_report_period(
        year=year,
        month=month,
    )
    report_payload = create_monthly_report_payload(
        year=resolved_year,
        month=resolved_month,
    )
    artifact = build_monthly_report_artifact(
        report_payload,
        pdf_renderer=pdf_renderer,
    )
    logger.info(
        "report_pipeline_monthly_artifact_built",
        "Built monthly report artifact for reporter request.",
        {
            "period": f"{resolved_year}-{resolved_month:02d}",
            "filename": artifact["filename"],
            "size_bytes": len(artifact["pdf_bytes"]),
        },
    )
    return {
        "schema_version": REPORT_ARTIFACT_SCHEMA_VERSION,
        "report_kind": "monthly_pdf",
        "period": f"{resolved_year}-{resolved_month:02d}",
        "filename": artifact["filename"],
        "html": artifact["html"],
        "pdf_bytes": artifact["pdf_bytes"],
        "payload": artifact["payload"],
        "narrative": artifact["narrative"],
        "charts": artifact["charts"],
    }
