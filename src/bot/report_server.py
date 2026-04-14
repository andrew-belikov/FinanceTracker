from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from common.logging_setup import get_logger


REPORTER_HOST = os.getenv("REPORTER_HOST", "0.0.0.0").strip() or "0.0.0.0"
REPORTER_PORT = int(os.getenv("REPORTER_PORT", "8088"))
REPORTER_MAX_BODY_BYTES = int(os.getenv("REPORTER_MAX_BODY_BYTES", "65536"))
REPORT_SCHEMA_VERSION = "monthly_report_stub.v1"
REPORT_PDF_ENGINE = "placeholder"
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"

logger = get_logger(__name__)


class ReportRequestError(ValueError):
    pass


def resolve_monthly_report_period(
    *,
    year: int | None = None,
    month: int | None = None,
) -> tuple[int, int]:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    current = datetime.now(ZoneInfo(TZ_NAME))
    resolved_year = year if year is not None else current.year
    resolved_month = month if month is not None else current.month

    if resolved_year < 1900 or resolved_year > 2100:
        raise ReportRequestError("Поле year должно быть в диапазоне 1900..2100.")
    if resolved_month < 1 or resolved_month > 12:
        raise ReportRequestError("Поле month должно быть в диапазоне 1..12.")

    return resolved_year, resolved_month


def build_report_health_payload() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "reporter",
        "schema_version": REPORT_SCHEMA_VERSION,
        "pdf_engine": REPORT_PDF_ENGINE,
        "timezone": TZ_NAME,
        "max_body_bytes": REPORTER_MAX_BODY_BYTES,
    }


def build_monthly_pdf_stub_response(payload: dict[str, Any] | None) -> dict[str, Any]:
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
    return {
        "status": "not_implemented",
        "report_kind": "monthly_pdf",
        "period": period,
        "schema_version": REPORT_SCHEMA_VERSION,
        "message": "Monthly PDF generation is not implemented yet.",
        "request_keys": sorted(request_payload.keys()),
    }


def _normalize_path(raw_path: str) -> str:
    normalized = urlsplit(raw_path).path or "/"
    if normalized != "/" and normalized.endswith("/"):
        return normalized.rstrip("/")
    return normalized


class ReporterHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class ReporterRequestHandler(BaseHTTPRequestHandler):
    server_version = "FinanceTrackerReporter/0.1"
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover - disable stderr logging
        return

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_request_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length", "0").strip() or "0"
        try:
            content_length = int(raw_length)
        except ValueError as exc:
            raise ValueError("invalid Content-Length header") from exc

        if content_length < 0:
            raise ValueError("negative Content-Length header")
        if content_length > REPORTER_MAX_BODY_BYTES:
            raise ValueError("request body is too large")
        if content_length == 0:
            return b""
        return self.rfile.read(content_length)

    def _read_json_body(self) -> dict[str, Any] | None:
        raw_body = self._read_request_body()
        if not raw_body:
            return None

        content_type = (self.headers.get("Content-Type") or "").lower()
        if content_type and "application/json" not in content_type:
            raise ValueError("expected application/json request body")

        parsed = json.loads(raw_body.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("expected a JSON object request body")
        return parsed

    def do_GET(self) -> None:  # pragma: no cover - covered through integration tests
        path = _normalize_path(self.path)
        if path == "/healthz":
            self._send_json(HTTPStatus.OK, build_report_health_payload())
            return

        self._send_json(
            HTTPStatus.NOT_FOUND,
            {
                "status": "error",
                "error": "not_found",
                "path": path,
            },
        )

    def do_POST(self) -> None:  # pragma: no cover - covered through integration tests
        path = _normalize_path(self.path)
        if path != "/reports/monthly/pdf":
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {
                    "status": "error",
                    "error": "not_found",
                    "path": path,
                },
            )
            return

        try:
            request_json = self._read_json_body()
            response_payload = build_monthly_pdf_stub_response(request_json)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, ReportRequestError) as exc:
            logger.warning(
                "reporter_monthly_pdf_request_rejected",
                "Rejected monthly PDF request.",
                {
                    "path": path,
                    "error": str(exc),
                },
            )
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "error",
                    "error": "invalid_request",
                    "message": str(exc),
                },
            )
            return

        logger.info(
            "reporter_monthly_pdf_requested",
            "Monthly PDF request accepted in stub mode.",
            {
                "path": path,
                "request_keys": response_payload["request_keys"],
            },
        )
        self._send_json(HTTPStatus.NOT_IMPLEMENTED, response_payload)


def build_reporter_server(host: str | None = None, port: int | None = None) -> ReporterHTTPServer:
    resolved_host = (host or REPORTER_HOST).strip() or REPORTER_HOST
    resolved_port = int(port if port is not None else REPORTER_PORT)
    return ReporterHTTPServer((resolved_host, resolved_port), ReporterRequestHandler)


def main() -> int:
    server: ReporterHTTPServer | None = None
    try:
        server = build_reporter_server()
    except OSError as exc:
        logger.exception(
            "reporter_server_bind_failed",
            "Reporter server failed to bind socket.",
            {
                "host": REPORTER_HOST,
                "port": REPORTER_PORT,
                "error": str(exc),
            },
        )
        return 1

    try:
        logger.info(
            "reporter_server_starting",
            "Reporter server starting.",
            {
                "host": REPORTER_HOST,
                "port": REPORTER_PORT,
            },
        )
        server.serve_forever(poll_interval=0.5)
        return 0
    except KeyboardInterrupt:
        logger.info(
            "reporter_server_shutdown_requested",
            "Reporter server shutdown requested.",
            {
                "host": REPORTER_HOST,
                "port": REPORTER_PORT,
            },
        )
        return 0
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        logger.exception(
            "reporter_server_failed",
            "Reporter server terminated with an unhandled exception.",
            {
                "host": REPORTER_HOST,
                "port": REPORTER_PORT,
                "error": str(exc),
            },
        )
        return 1
    finally:
        if server is not None:
            server.server_close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "reporter_entrypoint_failed",
            "Reporter server entrypoint terminated with an unhandled exception.",
        )
        raise SystemExit(1)
