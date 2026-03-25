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

from common.logging_setup import configure_logging, get_logger, relay_text_stream
from xray_client.render_config import build_config


STATUS_FILE = "/tmp/xray-client-status.json"
CONFIG_FILE = "/tmp/xray-client-config.json"


os.environ.setdefault("APP_SERVICE", "xray_client")
configure_logging()
logger = get_logger(__name__)


def is_enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def write_status(payload: dict) -> None:
    with open(STATUS_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)


def wait_for_proxy(port: int, timeout_seconds: float, proc: subprocess.Popen[str]) -> bool:
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


def stop_process(proc: subprocess.Popen[str], timeout_seconds: float = 10.0) -> None:
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout_seconds)


def main() -> int:
    proxy_enabled = is_enabled(os.getenv("BOT_PROXY_ENABLED"))
    listen_port = int(os.getenv("XRAY_LOCAL_PROXY_PORT", "3128"))

    if not proxy_enabled:
        write_status({"mode": "disabled"})
        logger.info(
            "xray_proxy_disabled",
            "Xray proxy mode is disabled.",
            {"listen_port": listen_port},
        )
        return 0

    vless_url = os.getenv("BOT_VLESS_URL", "").strip()
    if not vless_url:
        logger.error(
            "xray_missing_vless_url",
            "Xray proxy is enabled but BOT_VLESS_URL is empty.",
            {"listen_port": listen_port},
        )
        return 1

    config, link = build_config(vless_url, listen_port=listen_port)
    with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=True, indent=2)
        handle.write("\n")

    logger.info(
        "xray_config_rendered",
        "Rendered Xray client configuration.",
        {
            "listen_port": listen_port,
            "link_summary": link.masked_summary(),
            "config_file": CONFIG_FILE,
        },
    )
    logger.info(
        "xray_process_starting",
        "Starting local Xray proxy process.",
        {"listen_port": listen_port},
    )

    proc = subprocess.Popen(
        ["/usr/local/bin/xray", "run", "-config", CONFIG_FILE],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    relay_threads = []
    if proc.stdout is not None:
        relay_threads.append(
            relay_text_stream(
                logger,
                proc.stdout,
                "xray_process_output",
                stream_name="stdout",
                ctx={"child_process": "xray"},
            )
        )
    if proc.stderr is not None:
        relay_threads.append(
            relay_text_stream(
                logger,
                proc.stderr,
                "xray_process_output",
                stream_name="stderr",
                ctx={"child_process": "xray"},
            )
        )

    def stop_child(signum: int, _frame) -> None:
        logger.info(
            "xray_signal_received",
            "Xray client entrypoint received a shutdown signal.",
            {"signal": signum},
        )
        stop_process(proc)

    signal.signal(signal.SIGTERM, stop_child)
    signal.signal(signal.SIGINT, stop_child)

    if not wait_for_proxy(listen_port, timeout_seconds=20.0, proc=proc):
        logger.error(
            "xray_proxy_unavailable",
            "Local Xray proxy endpoint did not become ready in time.",
            {"listen_port": listen_port},
        )
        stop_process(proc)
        for thread in relay_threads:
            thread.join(timeout=1.0)
        return 1

    write_status({"mode": "enabled", "port": listen_port})
    logger.info(
        "xray_proxy_ready",
        "Local Xray proxy endpoint is ready.",
        {"listen_port": listen_port},
    )

    smoke_ok, smoke_details = run_smoke_through_proxy(listen_port)
    smoke_ctx = {
        "listen_port": listen_port,
        "ok": smoke_ok,
        "details": smoke_details,
    }
    if smoke_ok:
        logger.info(
            "xray_telegram_smoke_completed",
            "Xray proxy Telegram smoke test succeeded.",
            smoke_ctx,
        )
    else:
        logger.error(
            "xray_telegram_smoke_failed",
            "Xray proxy Telegram smoke test failed.",
            smoke_ctx,
        )

    return_code = proc.wait()
    for thread in relay_threads:
        thread.join(timeout=1.0)

    exit_ctx = {"return_code": return_code}
    if return_code == 0:
        logger.info("xray_process_exited", "Xray process exited cleanly.", exit_ctx)
    else:
        logger.error("xray_process_exited", "Xray process exited with a non-zero code.", exit_ctx)
    return return_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "xray_entrypoint_failed",
            "Xray client entrypoint terminated with an unhandled exception.",
        )
        raise SystemExit(1)
