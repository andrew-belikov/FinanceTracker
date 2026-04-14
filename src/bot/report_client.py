from __future__ import annotations

import json
import os
import tempfile
from typing import Any
from urllib import error, request

from common.logging_setup import get_logger


REPORTER_INTERNAL_URL = os.getenv("REPORTER_INTERNAL_URL", "http://reporter:8088").strip() or "http://reporter:8088"
REPORTER_REQUEST_TIMEOUT_SECONDS = float(os.getenv("REPORTER_REQUEST_TIMEOUT_SECONDS", "180").strip() or "180")

logger = get_logger(__name__)


class ReporterClientError(RuntimeError):
    pass


def _parse_filename(headers) -> str:
    content_disposition = headers.get("Content-Disposition") or ""
    marker = "filename="
    if marker in content_disposition:
        raw_value = content_disposition.split(marker, 1)[1].strip()
        return raw_value.strip('"') or "monthly_report.pdf"
    return "monthly_report.pdf"


def request_monthly_report_pdf(
    *,
    year: int | None = None,
    month: int | None = None,
) -> tuple[str, str]:
    payload: dict[str, Any] = {}
    if year is not None:
        payload["year"] = year
    if month is not None:
        payload["month"] = month

    url = f"{REPORTER_INTERNAL_URL.rstrip('/')}/reports/monthly/pdf"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    http_request = request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/pdf, application/json",
        },
        method="POST",
    )

    logger.info(
        "report_client_monthly_pdf_requested",
        "Requesting monthly PDF from reporter.",
        {
            "url": url,
            "request_keys": sorted(payload.keys()),
            "timeout_seconds": REPORTER_REQUEST_TIMEOUT_SECONDS,
        },
    )

    try:
        with request.urlopen(http_request, timeout=REPORTER_REQUEST_TIMEOUT_SECONDS) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            pdf_bytes = response.read()
            filename = _parse_filename(response.headers)
    except error.HTTPError as exc:
        message = f"Reporter вернул HTTP {exc.code}."
        try:
            response_payload = json.loads(exc.read().decode("utf-8"))
            message = response_payload.get("message") or response_payload.get("error") or message
        except Exception:
            pass
        logger.warning(
            "report_client_monthly_pdf_http_failed",
            "Reporter rejected monthly PDF request.",
            {
                "url": url,
                "status_code": exc.code,
                "message": message,
            },
        )
        raise ReporterClientError(message) from exc
    except Exception as exc:
        logger.exception(
            "report_client_monthly_pdf_failed",
            "Reporter monthly PDF request failed.",
            {
                "url": url,
                "request_keys": sorted(payload.keys()),
            },
        )
        raise ReporterClientError("Не удалось связаться с reporter для PDF-отчёта.") from exc

    if "application/pdf" not in content_type:
        logger.warning(
            "report_client_monthly_pdf_invalid_content_type",
            "Reporter returned unexpected content type for monthly PDF.",
            {
                "url": url,
                "content_type": content_type,
            },
        )
        raise ReporterClientError("Reporter вернул неожиданный ответ вместо PDF.")

    temp_file = tempfile.NamedTemporaryFile(prefix="fintracker_monthly_", suffix=".pdf", delete=False)
    temp_file.write(pdf_bytes)
    temp_file.close()

    logger.info(
        "report_client_monthly_pdf_received",
        "Reporter monthly PDF request completed.",
        {
            "url": url,
            "filename": filename,
            "size_bytes": len(pdf_bytes),
            "path": temp_file.name,
        },
    )
    return temp_file.name, filename
