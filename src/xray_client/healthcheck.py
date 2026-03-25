from __future__ import annotations

import json
import os
import socket
import sys

from common.logging_setup import configure_logging, get_logger


STATUS_FILE = "/tmp/xray-client-status.json"


os.environ.setdefault("APP_SERVICE", "xray_client")
configure_logging()
logger = get_logger(__name__)


def load_status() -> dict:
    with open(STATUS_FILE, "r", encoding="utf-8") as handle:
        return json.load(handle)


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
            return 0
    except OSError as exc:
        logger.error(
            "xray_healthcheck_proxy_unreachable",
            "Healthcheck could not reach the local proxy endpoint.",
            {"port": port, "error": str(exc)},
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
