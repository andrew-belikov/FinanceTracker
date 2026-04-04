from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time

from common.logging_setup import configure_logging, get_logger, relay_text_stream
from xray_client.render_config import build_config


STATUS_FILE = "/tmp/xray-client-status.json"
CONFIG_FILE = "/tmp/xray-client-config.json"
DEFAULT_HEALTHCHECK_URL = "https://api.ipify.org"
DEFAULT_PROXY_SCHEME = "socks5h"


os.environ.setdefault("APP_SERVICE", "xray_client")
configure_logging()
logger = get_logger(__name__)


def is_enabled(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def write_status(payload: dict) -> None:
    with open(STATUS_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)


def iter_vless_candidates(primary_url: str, fallback_url: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for role, raw_url in (
        ("primary", primary_url.strip()),
        ("fallback", fallback_url.strip()),
    ):
        if not raw_url or raw_url in seen_urls:
            continue
        seen_urls.add(raw_url)
        candidates.append((role, raw_url))
    return candidates


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


def run_smoke_through_proxy(
    port: int,
    target_url: str,
    proxy_scheme: str = DEFAULT_PROXY_SCHEME,
) -> tuple[bool, str]:
    completed = subprocess.run(
        build_proxy_check_command(port, target_url, proxy_scheme=proxy_scheme),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return True, f"target_url={target_url}"

    details = completed.stderr.strip() or completed.stdout.strip() or f"exit_code={completed.returncode}"
    return False, details


def stop_process(proc: subprocess.Popen[str], timeout_seconds: float = 10.0) -> None:
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout_seconds)


def start_relay_threads(proc: subprocess.Popen[str]) -> list:
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
    return relay_threads


def join_relay_threads(relay_threads: list) -> None:
    for thread in relay_threads:
        thread.join(timeout=1.0)


def main() -> int:
    proxy_enabled = is_enabled(os.getenv("BOT_PROXY_ENABLED"))
    listen_port = int(os.getenv("XRAY_LOCAL_PROXY_PORT", "1080"))
    healthcheck_url = os.getenv("XRAY_HEALTHCHECK_URL", DEFAULT_HEALTHCHECK_URL).strip() or DEFAULT_HEALTHCHECK_URL

    if not proxy_enabled:
        write_status({"mode": "disabled"})
        logger.info(
            "xray_proxy_disabled",
            "Xray proxy mode is disabled.",
            {"listen_port": listen_port},
        )
        return 0

    vless_url = os.getenv("BOT_VLESS_URL", "")
    fallback_vless_url = os.getenv("BOT_VLESS_FALLBACK_URL", "")
    candidates = iter_vless_candidates(vless_url, fallback_vless_url)
    if not candidates:
        logger.error(
            "xray_missing_vless_url",
            "Xray proxy is enabled but both BOT_VLESS_URL and BOT_VLESS_FALLBACK_URL are empty.",
            {"listen_port": listen_port},
        )
        return 1
    log_level = os.getenv("XRAY_LOG_LEVEL", "warning").strip() or "warning"
    proc = None
    relay_threads: list = []
    active_candidate_role: str | None = None
    active_link_summary: str | None = None

    def stop_child(signum: int, _frame) -> None:
        logger.info(
            "xray_signal_received",
            "Xray client entrypoint received a shutdown signal.",
            {"signal": signum},
        )
        stop_process(proc)

    signal.signal(signal.SIGTERM, stop_child)
    signal.signal(signal.SIGINT, stop_child)
    for index, (candidate_role, candidate_url) in enumerate(candidates, start=1):
        try:
            config, link = build_config(
                candidate_url,
                listen_port=listen_port,
                log_level=log_level,
            )
        except Exception:
            logger.exception(
                "xray_config_render_failed",
                "Failed to render Xray configuration for candidate VLESS URL.",
                {
                    "listen_port": listen_port,
                    "candidate_role": candidate_role,
                    "candidate_index": index,
                    "candidate_count": len(candidates),
                },
            )
            continue

        with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(config, handle, ensure_ascii=True, indent=2)
            handle.write("\n")

        logger.info(
            "xray_config_rendered",
            "Rendered Xray client configuration.",
            {
                "listen_port": listen_port,
                "candidate_role": candidate_role,
                "candidate_index": index,
                "candidate_count": len(candidates),
                "link_summary": link.masked_summary(),
                "config_file": CONFIG_FILE,
            },
        )
        logger.info(
            "xray_process_starting",
            "Starting local Xray proxy process.",
            {
                "listen_port": listen_port,
                "candidate_role": candidate_role,
                "candidate_index": index,
                "candidate_count": len(candidates),
            },
        )

        proc = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", CONFIG_FILE],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        relay_threads = start_relay_threads(proc)

        if not wait_for_proxy(listen_port, timeout_seconds=20.0, proc=proc):
            logger.error(
                "xray_proxy_unavailable",
                "Local Xray proxy endpoint did not become ready in time.",
                {
                    "listen_port": listen_port,
                    "candidate_role": candidate_role,
                    "candidate_index": index,
                    "candidate_count": len(candidates),
                },
            )
            stop_process(proc)
            join_relay_threads(relay_threads)
            proc = None
            relay_threads = []
            continue

        logger.info(
            "xray_proxy_ready",
            "Local Xray proxy endpoint is ready.",
            {
                "listen_port": listen_port,
                "candidate_role": candidate_role,
                "candidate_index": index,
                "candidate_count": len(candidates),
            },
        )

        smoke_ok, smoke_details = run_smoke_through_proxy(
            listen_port,
            target_url=healthcheck_url,
            proxy_scheme=DEFAULT_PROXY_SCHEME,
        )
        smoke_ctx = {
            "listen_port": listen_port,
            "candidate_role": candidate_role,
            "candidate_index": index,
            "candidate_count": len(candidates),
            "ok": smoke_ok,
            "details": smoke_details,
            "target_url": healthcheck_url,
        }
        if smoke_ok:
            active_candidate_role = candidate_role
            active_link_summary = link.masked_summary()
            write_status(
                {
                    "mode": "enabled",
                    "port": listen_port,
                    "proxy_scheme": DEFAULT_PROXY_SCHEME,
                    "healthcheck_url": healthcheck_url,
                    "active_link_role": candidate_role,
                    "link_summary": active_link_summary,
                }
            )
            logger.info(
                "xray_proxy_smoke_completed",
                "Xray proxy smoke test succeeded.",
                smoke_ctx,
            )
            break

        logger.error(
            "xray_proxy_smoke_failed",
            "Xray proxy smoke test failed.",
            smoke_ctx,
        )
        stop_process(proc)
        join_relay_threads(relay_threads)
        proc = None
        relay_threads = []
        if index < len(candidates):
            logger.warning(
                "xray_proxy_fallback_attempt_scheduled",
                "Current VLESS candidate failed; trying the next candidate.",
                {
                    "listen_port": listen_port,
                    "failed_candidate_role": candidate_role,
                    "next_candidate_role": candidates[index][0],
                },
            )

    if proc is None or active_candidate_role is None:
        logger.error(
            "xray_all_candidates_failed",
            "All configured VLESS candidates failed to start a working proxy route.",
            {"listen_port": listen_port, "candidate_count": len(candidates)},
        )
        return 1

    return_code = proc.wait()
    join_relay_threads(relay_threads)

    exit_ctx = {
        "return_code": return_code,
        "active_link_role": active_candidate_role,
        "link_summary": active_link_summary,
    }
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
