from __future__ import annotations

import hmac
import json
import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from common.logging_setup import get_logger
from report_pipeline import (
    ReportRequestError,
    build_monthly_report_artifact_for_request,
    build_report_health_payload,
)
from report_render import ReportRenderError


REPORTER_HOST = os.getenv("REPORTER_HOST", "0.0.0.0").strip() or "0.0.0.0"
REPORTER_PORT = int(os.getenv("REPORTER_PORT", "8088"))
REPORTER_MAX_BODY_BYTES = int(os.getenv("REPORTER_MAX_BODY_BYTES", "65536"))
REPORTER_SERVICE_KEY = os.getenv("REPORTER_SERVICE_KEY", "").strip()
REPORTER_MAX_CONCURRENT_REQUESTS = int(os.getenv("REPORTER_MAX_CONCURRENT_REQUESTS", "2"))
REPORTER_SOCKET_TIMEOUT_SECONDS = float(os.getenv("REPORTER_SOCKET_TIMEOUT_SECONDS", "10"))
REPORTER_REQUEST_TIMEOUT_SECONDS = float(os.getenv("REPORTER_REQUEST_TIMEOUT_SECONDS", "180"))
MONTHLY_REPORT_BUILDER = build_monthly_report_artifact_for_request

logger = get_logger(__name__)


def _normalize_path(raw_path: str) -> str:
    normalized = urlsplit(raw_path).path or "/"
    if normalized != "/" and normalized.endswith("/"):
        return normalized.rstrip("/")
    return normalized


class ReporterHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        handler_class,
        *,
        service_key: str,
        max_concurrent_requests: int,
        socket_timeout_seconds: float,
        request_timeout_seconds: float,
    ):
        if len(service_key) < 16:
            raise ValueError("REPORTER_SERVICE_KEY must contain at least 16 characters")
        if max_concurrent_requests < 1 or max_concurrent_requests > 32:
            raise ValueError("REPORTER_MAX_CONCURRENT_REQUESTS must be between 1 and 32")
        if socket_timeout_seconds <= 0 or request_timeout_seconds <= 0:
            raise ValueError("Reporter timeouts must be positive")
        self.service_key = service_key
        self.request_budget = threading.BoundedSemaphore(max_concurrent_requests)
        self.connection_budget = threading.BoundedSemaphore(max_concurrent_requests * 2)
        self.socket_timeout_seconds = socket_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds
        super().__init__(server_address, handler_class)
        self.request_pool = ThreadPoolExecutor(
            max_workers=max_concurrent_requests * 2,
            thread_name_prefix="reporter-request",
        )
        self.worker_pool = ThreadPoolExecutor(
            max_workers=max_concurrent_requests,
            thread_name_prefix="reporter-builder",
        )

    def process_request(self, request, client_address) -> None:
        if not self.connection_budget.acquire(blocking=False):
            self.shutdown_request(request)
            return

        def process_bounded_request() -> None:
            try:
                self.process_request_thread(request, client_address)
            finally:
                self.connection_budget.release()

        self.request_pool.submit(process_bounded_request)

    def server_close(self) -> None:
        super().server_close()
        self.request_pool.shutdown(wait=False, cancel_futures=True)
        self.worker_pool.shutdown(wait=False, cancel_futures=True)


class ReporterRequestHandler(BaseHTTPRequestHandler):
    server_version = "FinanceTrackerReporter/0.1"
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(self.server.socket_timeout_seconds)

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

    def _send_pdf(self, pdf_bytes: bytes, *, filename: str, period: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", str(len(pdf_bytes)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("X-Report-Period", period)
        self.end_headers()
        self.wfile.write(pdf_bytes)

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

        supplied_key = self.headers.get("X-Reporter-Service-Key")
        if supplied_key is None:
            self.close_connection = True
            self._send_json(
                HTTPStatus.UNAUTHORIZED,
                {"status": "error", "error": "authentication_required"},
            )
            return
        if not hmac.compare_digest(supplied_key.encode("utf-8"), self.server.service_key.encode("utf-8")):
            self.close_connection = True
            self._send_json(
                HTTPStatus.FORBIDDEN,
                {"status": "error", "error": "authentication_failed"},
            )
            return

        if not self.server.request_budget.acquire(blocking=False):
            self.close_connection = True
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"status": "error", "error": "reporter_overloaded"},
            )
            return

        budget_owned = True
        future = None
        body_read = False

        try:
            request_json = self._read_json_body()
            body_read = True
        except socket.timeout:
            self.close_connection = True
            self._send_json(
                HTTPStatus.REQUEST_TIMEOUT,
                {"status": "error", "error": "request_timeout"},
            )
            return
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
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
        finally:
            if not body_read and budget_owned:
                self.server.request_budget.release()
                budget_owned = False

        try:
            future = self.server.worker_pool.submit(MONTHLY_REPORT_BUILDER, request_json)
            future.add_done_callback(lambda _future: self.server.request_budget.release())
            budget_owned = False
            artifact = future.result(timeout=self.server.request_timeout_seconds)
        except FutureTimeoutError:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"status": "error", "error": "report_timeout"},
            )
            return
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, ReportRequestError) as exc:
            error_code = "invalid_request" if isinstance(exc, ReportRequestError) else "report_unavailable"
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
                    "error": error_code,
                    "message": str(exc),
                },
            )
            return
        except ReportRenderError as exc:
            logger.exception(
                "reporter_monthly_pdf_render_failed",
                "Reporter failed to render monthly PDF.",
                {
                    "path": path,
                    "error": str(exc),
                },
            )
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "status": "error",
                    "error": "report_render_failed",
                    "message": str(exc),
                },
            )
            return
        except Exception as exc:
            logger.exception(
                "reporter_monthly_pdf_failed",
                "Reporter failed to build monthly PDF.",
                {
                    "path": path,
                    "error": str(exc),
                },
            )
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "status": "error",
                    "error": "report_failed",
                    "message": "Не удалось собрать monthly PDF report.",
                },
            )
            return
        finally:
            if budget_owned:
                self.server.request_budget.release()

        logger.info(
            "reporter_monthly_pdf_built",
            "Reporter built monthly PDF response.",
            {
                "path": path,
                "filename": artifact["filename"],
                "period": artifact["period"],
                "size_bytes": len(artifact["pdf_bytes"]),
            },
        )
        self._send_pdf(
            artifact["pdf_bytes"],
            filename=artifact["filename"],
            period=artifact["period"],
        )


def build_reporter_server(
    host: str | None = None,
    port: int | None = None,
    *,
    service_key: str | None = None,
    max_concurrent_requests: int | None = None,
    socket_timeout_seconds: float | None = None,
    request_timeout_seconds: float | None = None,
) -> ReporterHTTPServer:
    resolved_host = (host or REPORTER_HOST).strip() or REPORTER_HOST
    resolved_port = int(port if port is not None else REPORTER_PORT)
    return ReporterHTTPServer(
        (resolved_host, resolved_port),
        ReporterRequestHandler,
        service_key=service_key if service_key is not None else REPORTER_SERVICE_KEY,
        max_concurrent_requests=(
            max_concurrent_requests
            if max_concurrent_requests is not None
            else REPORTER_MAX_CONCURRENT_REQUESTS
        ),
        socket_timeout_seconds=(
            socket_timeout_seconds
            if socket_timeout_seconds is not None
            else REPORTER_SOCKET_TIMEOUT_SECONDS
        ),
        request_timeout_seconds=(
            request_timeout_seconds
            if request_timeout_seconds is not None
            else REPORTER_REQUEST_TIMEOUT_SECONDS
        ),
    )


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
