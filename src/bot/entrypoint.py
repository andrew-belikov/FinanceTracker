from __future__ import annotations

import os
import socket
import sys
import time
from typing import Iterable
from urllib.parse import urlparse

from proxy_smoke import run_startup_smoke


PROXY_ENV_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
REQUIRED_NO_PROXY = ("localhost", "127.0.0.1", "db", "tracker", "xray-client")


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
    proxy_endpoint = os.getenv("BOT_PROXY_ENDPOINT", "http://xray-client:3128").strip()
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
    port = parsed.port or 3128
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except OSError:
            time.sleep(1.0)
    return False


def main() -> int:
    proxy_enabled, proxy_endpoint, no_proxy = configure_proxy_env()
    print(
        "bot_entrypoint proxy_enabled=%s proxy_endpoint=%s no_proxy=%s"
        % (str(proxy_enabled).lower(), proxy_endpoint if proxy_enabled else "<disabled>", no_proxy),
        flush=True,
    )

    if proxy_enabled:
        print(f"bot_entrypoint waiting_for_proxy endpoint={proxy_endpoint}", flush=True)
        if not wait_for_proxy_endpoint(proxy_endpoint):
            print(f"bot_entrypoint proxy_unavailable endpoint={proxy_endpoint}", file=sys.stderr, flush=True)
            return 1

    run_startup_smoke()
    os.execvp("python", ["python", "-u", "bot.py"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
