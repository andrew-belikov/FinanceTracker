from __future__ import annotations

import json
import os
import socket
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse


def is_enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def build_telegram_probe_url() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if token:
        return f"https://api.telegram.org/bot{token}/getMe"
    return "https://api.telegram.org"


def probe_tcp(host: str, port: int, timeout: float) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "connected"
    except OSError as exc:
        return False, str(exc)


def probe_telegram(timeout: float) -> tuple[bool, str]:
    request = urllib.request.Request(build_telegram_probe_url(), headers={"User-Agent": "FinanceTrackerBotProxySmoke/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status_code = getattr(response, "status", 200)
            return True, f"http_status={status_code}"
    except urllib.error.HTTPError as exc:
        return True, f"http_status={exc.code}"
    except Exception as exc:  # pragma: no cover - network-dependent
        return False, str(exc)


def resolve_proxy_target() -> tuple[str | None, int | None, str]:
    proxy_url = (
        os.getenv("HTTPS_PROXY")
        or os.getenv("https_proxy")
        or os.getenv("ALL_PROXY")
        or os.getenv("all_proxy")
        or os.getenv("BOT_PROXY_ENDPOINT", "")
    ).strip()
    if not proxy_url:
        return None, None, ""
    parsed = urlparse(proxy_url)
    return parsed.hostname, parsed.port, proxy_url


def collect_results() -> tuple[int, list[dict[str, str]]]:
    results: list[dict[str, str]] = []
    exit_code = 0

    proxy_enabled = is_enabled(os.getenv("BOT_PROXY_ENABLED"))
    proxy_host, proxy_port, proxy_url = resolve_proxy_target()
    results.append(
        {
            "check": "proxy_mode",
            "ok": "true",
            "details": "enabled" if proxy_enabled else "disabled",
        }
    )

    if proxy_enabled and proxy_host and proxy_port:
        ok, details = probe_tcp(proxy_host, proxy_port, timeout=5.0)
        results.append(
            {
                "check": "proxy_endpoint",
                "ok": str(ok).lower(),
                "details": f"{proxy_host}:{proxy_port} {details}",
            }
        )
        if not ok:
            exit_code = 1
    elif proxy_enabled:
        results.append(
            {
                "check": "proxy_endpoint",
                "ok": "false",
                "details": f"invalid proxy url: {proxy_url or '<empty>'}",
            }
        )
        exit_code = 1

    db_host = os.getenv("DB_HOST", "db").strip()
    db_port = int(os.getenv("DB_PORT", "5432"))
    ok, details = probe_tcp(db_host, db_port, timeout=5.0)
    results.append(
        {
            "check": "db_tcp_direct",
            "ok": str(ok).lower(),
            "details": f"{db_host}:{db_port} {details}",
        }
    )
    if not ok:
        exit_code = 1

    ok, details = probe_telegram(timeout=15.0)
    results.append(
        {
            "check": "telegram_api",
            "ok": str(ok).lower(),
            "details": details,
        }
    )
    if not ok:
        exit_code = 1

    return exit_code, results


def run_startup_smoke() -> int:
    exit_code, results = collect_results()
    print("bot_startup_smoke %s" % json.dumps(results, ensure_ascii=True, sort_keys=True), flush=True)
    return exit_code


def main() -> int:
    return run_startup_smoke()


if __name__ == "__main__":
    sys.exit(main())
