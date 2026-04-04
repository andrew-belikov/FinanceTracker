from __future__ import annotations

import json
import os
import socket
import subprocess
import sys

from common.logging_setup import configure_logging, get_logger


STATUS_FILE = "/tmp/xray-client-status.json"
DEFAULT_HEALTHCHECK_URL = "https://api.ipify.org"
DEFAULT_PROXY_SCHEME = "socks5h"


os.environ.setdefault("APP_SERVICE", "xray_client")
configure_logging()
logger = get_logger(__name__)


def load_status() -> dict:
    with open(STATUS_FILE, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_proxy_check_command(port: int, target_url: str, proxy_scheme: str = DEFAULT_PROXY_SCHEME) -> list[str]:
    if proxy_scheme not in {"socks5", "socks5h"}:
        raise ValueError(f"Unsupported proxy scheme: {proxy_scheme}")
    return [
        "curl",
        "--max-time",
        "10",
        "--socks5-hostname",
        f"127.0.0.1:{port}",
        "--fail",
        "--silent",
        "--show-error",
        "--output",
        "/dev/null",
        target_url,
    ]


def run_proxy_request_smoke(port: int, target_url: str, proxy_scheme: str = DEFAULT_PROXY_SCHEME) -> tuple[bool, str]:
    completed = subprocess.run(
        build_proxy_check_command(port, target_url, proxy_scheme=proxy_scheme),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return True, ""

    details = completed.stderr.strip() or completed.stdout.strip() or f"exit_code={completed.returncode}"
    return False, details


def main() -> int:
    if not os.path.exists(STATUS_FILE):
        logger.error(
            "xray_healthcheck_status_missing",
            "Healthcheck status file is missing.",
            {"status_file": STATUS_FILE},
        )
        return 1

    try:
        status = load_status()
    except Exception:
        logger.exception(
            "xray_healthcheck_status_load_failed",
            "Healthcheck failed to load status file.",
            {"status_file": STATUS_FILE},
        )
        return 1

    mode = status.get("mode")
    if mode == "disabled":
        return 0

    if mode != "enabled":
        logger.error(
            "xray_healthcheck_invalid_mode",
            "Healthcheck status contains an unsupported mode.",
            {"mode": mode},
        )
        return 1

    try:
        port = int(status.get("port", 0))
    except (TypeError, ValueError):
        logger.error(
            "xray_healthcheck_invalid_port",
            "Healthcheck status contains a non-integer proxy port.",
            {"port": status.get("port")},
        )
        return 1
    if port <= 0:
        logger.error(
            "xray_healthcheck_invalid_port",
            "Healthcheck status contains an invalid proxy port.",
            {"port": port},
        )
        return 1

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3.0):
            pass
    except OSError as exc:
        logger.error(
            "xray_healthcheck_proxy_unreachable",
            "Healthcheck could not reach the local proxy endpoint.",
            {"port": port, "error": str(exc)},
        )
        return 1

    proxy_scheme = (status.get("proxy_scheme") or DEFAULT_PROXY_SCHEME).strip() or DEFAULT_PROXY_SCHEME
    healthcheck_url = (status.get("healthcheck_url") or DEFAULT_HEALTHCHECK_URL).strip() or DEFAULT_HEALTHCHECK_URL

    try:
        ok, details = run_proxy_request_smoke(
            port,
            healthcheck_url,
            proxy_scheme=proxy_scheme,
        )
    except ValueError:
        logger.exception(
            "xray_healthcheck_invalid_proxy_scheme",
            "Healthcheck status contains an unsupported proxy scheme.",
            {"proxy_scheme": proxy_scheme},
        )
        return 1

    if ok:
        return 0

    logger.error(
        "xray_healthcheck_proxy_request_failed",
        "Healthcheck could not complete an outbound request through the local proxy.",
        {
            "port": port,
            "proxy_scheme": proxy_scheme,
            "healthcheck_url": healthcheck_url,
            "details": details,
        },
    )
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "xray_healthcheck_unhandled_failure",
            "Xray healthcheck terminated with an unhandled exception.",
        )
        raise SystemExit(1)
