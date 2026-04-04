from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from typing import Iterable
from urllib.parse import urlparse

from common.logging_setup import configure_logging, get_logger
from proxy_smoke import run_startup_smoke


PROXY_ENV_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
REQUIRED_NO_PROXY = ("localhost", "127.0.0.1", "db", "tracker", "xray-client")
BOT_STARTUP_RETRY_EXIT_CODE = 76


configure_logging()
logger = get_logger(__name__)


def is_enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def merge_csv_values(values: Iterable[str]) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in values:
        for item in raw.split(","):
            candidate = item.strip()
            if not candidate:
                continue
            lowered = candidate.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(candidate)
    return ",".join(merged)


def configure_proxy_env() -> tuple[bool, str, str]:
    proxy_enabled = is_enabled(os.getenv("BOT_PROXY_ENABLED"))
    proxy_endpoint = os.getenv("BOT_PROXY_ENDPOINT", "socks5h://xray-client:1080").strip()
    no_proxy = merge_csv_values(
        [
            ",".join(REQUIRED_NO_PROXY),
            os.getenv("BOT_PROXY_NO_PROXY", ""),
            os.getenv("NO_PROXY", ""),
            os.getenv("no_proxy", ""),
        ]
    )

    os.environ["NO_PROXY"] = no_proxy
    os.environ["no_proxy"] = no_proxy

    if proxy_enabled:
        for key in PROXY_ENV_KEYS:
            os.environ[key] = proxy_endpoint
            os.environ[key.lower()] = proxy_endpoint
    else:
        for key in PROXY_ENV_KEYS:
            os.environ.pop(key, None)
            os.environ.pop(key.lower(), None)

    return proxy_enabled, proxy_endpoint, no_proxy


def wait_for_proxy_endpoint(proxy_endpoint: str, timeout_seconds: float = 30.0) -> bool:
    parsed = urlparse(proxy_endpoint)
    host = parsed.hostname or "xray-client"
    port = parsed.port or 1080
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except OSError:
            time.sleep(1.0)
    return False


def get_bot_startup_retry_delay_seconds() -> int:
    raw_value = (os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", "15").strip() or "15")
    return max(0, int(raw_value))


def should_retry_bot_process(exit_code: int, retry_exit_code: int = BOT_STARTUP_RETRY_EXIT_CODE) -> bool:
    return exit_code == retry_exit_code


def run_bot_process() -> int:
    completed = subprocess.run(["python", "-u", "bot.py"], check=False)
    return completed.returncode


def main() -> int:
    proxy_enabled, proxy_endpoint, no_proxy = configure_proxy_env()
    startup_retry_delay_seconds = get_bot_startup_retry_delay_seconds()
    logger.info(
        "bot_proxy_environment_configured",
        "Bot proxy environment configured.",
        {
            "proxy_enabled": proxy_enabled,
            "proxy_endpoint": proxy_endpoint if proxy_enabled else None,
            "no_proxy": no_proxy,
        },
    )

    if proxy_enabled:
        logger.info(
            "bot_proxy_wait_started",
            "Waiting for bot proxy endpoint.",
            {
                "proxy_endpoint": proxy_endpoint,
                "startup_retry_delay_seconds": startup_retry_delay_seconds,
            },
        )
        if not wait_for_proxy_endpoint(proxy_endpoint):
            logger.error(
                "bot_proxy_unavailable",
                "Bot proxy endpoint is unavailable.",
                {"proxy_endpoint": proxy_endpoint},
            )
            return 1

    run_startup_smoke()
    attempt = 1
    while True:
        logger.info(
            "bot_process_exec_started",
            "Starting bot process.",
            {"attempt": attempt},
        )
        exit_code = run_bot_process()
        if not should_retry_bot_process(exit_code):
            return exit_code

        logger.warning(
            "bot_process_retry_scheduled",
            "Bot process requested restart after Telegram transport failure.",
            {
                "attempt": attempt,
                "exit_code": exit_code,
                "retry_exit_code": BOT_STARTUP_RETRY_EXIT_CODE,
                "retry_delay_seconds": startup_retry_delay_seconds,
            },
        )
        time.sleep(startup_retry_delay_seconds)
        attempt += 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "bot_entrypoint_failed",
            "Bot entrypoint terminated with an unhandled exception.",
        )
        raise SystemExit(1)
