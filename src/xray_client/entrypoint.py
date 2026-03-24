from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

from render_config import build_config


STATUS_FILE = "/tmp/xray-client-status.json"
CONFIG_FILE = "/tmp/xray-client-config.json"


def is_enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def write_status(payload: dict) -> None:
    with open(STATUS_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)


def wait_for_proxy(port: int, timeout_seconds: float, proc: subprocess.Popen[bytes]) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def run_smoke_through_proxy(port: int) -> tuple[bool, str]:
    proxy_url = f"http://127.0.0.1:{port}"
    proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    opener = urllib.request.build_opener(proxy_handler)
    request = urllib.request.Request("https://api.telegram.org", headers={"User-Agent": "FinanceTrackerXrayClient/1.0"})
    try:
        with opener.open(request, timeout=15.0) as response:
            return True, f"http_status={getattr(response, 'status', 200)}"
    except urllib.error.HTTPError as exc:
        return True, f"http_status={exc.code}"
    except Exception as exc:  # pragma: no cover - network-dependent
        return False, str(exc)


def main() -> int:
    proxy_enabled = is_enabled(os.getenv("BOT_PROXY_ENABLED"))
    listen_port = int(os.getenv("XRAY_LOCAL_PROXY_PORT", "3128"))

    if not proxy_enabled:
        write_status({"mode": "disabled"})
        print("xray_client mode=disabled reason=BOT_PROXY_ENABLED=false", flush=True)
        return 0

    vless_url = os.getenv("BOT_VLESS_URL", "").strip()
    if not vless_url:
        print("xray_client mode=enabled error=BOT_VLESS_URL is empty", file=sys.stderr, flush=True)
        return 1

    config, link = build_config(vless_url, listen_port=listen_port)
    with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=True, indent=2)
        handle.write("\n")

    print(f"xray_client mode=enabled {link.masked_summary()}", flush=True)
    print(f"xray_client starting local_proxy=http://0.0.0.0:{listen_port}", flush=True)

    proc = subprocess.Popen(["/usr/local/bin/xray", "run", "-config", CONFIG_FILE])

    def stop_child(signum: int, _frame) -> None:
        print(f"xray_client received_signal={signum}", flush=True)
        if proc.poll() is None:
            proc.terminate()

    signal.signal(signal.SIGTERM, stop_child)
    signal.signal(signal.SIGINT, stop_child)

    if not wait_for_proxy(listen_port, timeout_seconds=20.0, proc=proc):
        print("xray_client proxy_endpoint_unavailable", file=sys.stderr, flush=True)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        return 1

    write_status({"mode": "enabled", "port": listen_port})
    print(f"xray_client proxy_endpoint_ready=http://0.0.0.0:{listen_port}", flush=True)

    smoke_ok, smoke_details = run_smoke_through_proxy(listen_port)
    print(
        "xray_client telegram_smoke ok=%s details=%s"
        % (str(smoke_ok).lower(), smoke_details),
        flush=True,
    )

    return proc.wait()


if __name__ == "__main__":
    sys.exit(main())
