from __future__ import annotations

from dataclasses import dataclass
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
DEFAULT_PROXY_READY_TIMEOUT_SECONDS = 20.0
DEFAULT_RUNTIME_SMOKE_INTERVAL_SECONDS = 15.0
DEFAULT_RUNTIME_SMOKE_FAILURE_THRESHOLD = 3
DEFAULT_PROCESS_POLL_INTERVAL_SECONDS = 1.0


os.environ.setdefault("APP_SERVICE", "xray_client")
configure_logging()
logger = get_logger(__name__)


@dataclass
class ActiveProxySession:
    proc: subprocess.Popen[str]
    relay_threads: list
    candidate_index: int
    candidate_role: str
    link_summary: str


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


def iter_candidate_indexes(start_index: int, candidate_count: int) -> list[int]:
    if candidate_count <= 0:
        return []

    normalized_start_index = start_index % candidate_count
    return [
        (normalized_start_index + offset) % candidate_count
        for offset in range(candidate_count)
    ]


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


def build_enabled_status(
    *,
    listen_port: int,
    healthcheck_url: str,
    candidate_role: str,
    link_summary: str,
) -> dict:
    return {
        "mode": "enabled",
        "port": listen_port,
        "proxy_scheme": DEFAULT_PROXY_SCHEME,
        "healthcheck_url": healthcheck_url,
        "active_link_role": candidate_role,
        "link_summary": link_summary,
    }


def stop_active_session(session: ActiveProxySession | None) -> None:
    if session is None:
        return
    stop_process(session.proc)
    join_relay_threads(session.relay_threads)


def log_candidate_rotation(
    *,
    listen_port: int,
    failed_candidate_role: str,
    next_candidate_role: str,
    reason: str,
) -> None:
    if reason == "startup":
        logger.warning(
            "xray_proxy_fallback_attempt_scheduled",
            "Current VLESS candidate failed; trying the next candidate.",
            {
                "listen_port": listen_port,
                "failed_candidate_role": failed_candidate_role,
                "next_candidate_role": next_candidate_role,
            },
        )
        return

    if reason == "runtime_smoke":
        logger.warning(
            "xray_runtime_failover_scheduled",
            "Active Xray route became unhealthy; trying the next configured candidate.",
            {
                "listen_port": listen_port,
                "failed_candidate_role": failed_candidate_role,
                "next_candidate_role": next_candidate_role,
            },
        )
        return

    if reason == "process_exit":
        logger.warning(
            "xray_process_restart_scheduled",
            "Active Xray process exited; trying the next configured candidate.",
            {
                "listen_port": listen_port,
                "failed_candidate_role": failed_candidate_role,
                "next_candidate_role": next_candidate_role,
            },
        )


def start_candidate(
    *,
    candidates: list[tuple[str, str]],
    candidate_index: int,
    listen_port: int,
    log_level: str,
    healthcheck_url: str,
) -> ActiveProxySession | None:
    candidate_role, candidate_url = candidates[candidate_index]
    candidate_count = len(candidates)
    candidate_position = candidate_index + 1

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
                "candidate_index": candidate_position,
                "candidate_count": candidate_count,
            },
        )
        return None

    with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=True, indent=2)
        handle.write("\n")

    logger.info(
        "xray_config_rendered",
        "Rendered Xray client configuration.",
        {
            "listen_port": listen_port,
            "candidate_role": candidate_role,
            "candidate_index": candidate_position,
            "candidate_count": candidate_count,
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
            "candidate_index": candidate_position,
            "candidate_count": candidate_count,
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

    if not wait_for_proxy(
        listen_port,
        timeout_seconds=DEFAULT_PROXY_READY_TIMEOUT_SECONDS,
        proc=proc,
    ):
        logger.error(
            "xray_proxy_unavailable",
            "Local Xray proxy endpoint did not become ready in time.",
            {
                "listen_port": listen_port,
                "candidate_role": candidate_role,
                "candidate_index": candidate_position,
                "candidate_count": candidate_count,
            },
        )
        stop_process(proc)
        join_relay_threads(relay_threads)
        return None

    logger.info(
        "xray_proxy_ready",
        "Local Xray proxy endpoint is ready.",
        {
            "listen_port": listen_port,
            "candidate_role": candidate_role,
            "candidate_index": candidate_position,
            "candidate_count": candidate_count,
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
        "candidate_index": candidate_position,
        "candidate_count": candidate_count,
        "ok": smoke_ok,
        "details": smoke_details,
        "target_url": healthcheck_url,
    }
    if not smoke_ok:
        logger.error(
            "xray_proxy_smoke_failed",
            "Xray proxy smoke test failed.",
            smoke_ctx,
        )
        stop_process(proc)
        join_relay_threads(relay_threads)
        return None

    write_status(
        build_enabled_status(
            listen_port=listen_port,
            healthcheck_url=healthcheck_url,
            candidate_role=candidate_role,
            link_summary=link.masked_summary(),
        )
    )
    logger.info(
        "xray_proxy_smoke_completed",
        "Xray proxy smoke test succeeded.",
        smoke_ctx,
    )
    return ActiveProxySession(
        proc=proc,
        relay_threads=relay_threads,
        candidate_index=candidate_index,
        candidate_role=candidate_role,
        link_summary=link.masked_summary(),
    )


def activate_candidate(
    *,
    candidates: list[tuple[str, str]],
    start_index: int,
    listen_port: int,
    log_level: str,
    healthcheck_url: str,
    rotation_reason: str,
) -> ActiveProxySession | None:
    attempt_order = iter_candidate_indexes(start_index, len(candidates))
    for order_index, candidate_index in enumerate(attempt_order):
        session = start_candidate(
            candidates=candidates,
            candidate_index=candidate_index,
            listen_port=listen_port,
            log_level=log_level,
            healthcheck_url=healthcheck_url,
        )
        if session is not None:
            return session

        if order_index + 1 >= len(attempt_order):
            continue

        failed_candidate_role = candidates[candidate_index][0]
        next_candidate_role = candidates[attempt_order[order_index + 1]][0]
        log_candidate_rotation(
            listen_port=listen_port,
            failed_candidate_role=failed_candidate_role,
            next_candidate_role=next_candidate_role,
            reason=rotation_reason,
        )

    return None


def monitor_active_candidate(
    session: ActiveProxySession,
    *,
    listen_port: int,
    healthcheck_url: str,
    check_interval_seconds: float = DEFAULT_RUNTIME_SMOKE_INTERVAL_SECONDS,
    failure_threshold: int = DEFAULT_RUNTIME_SMOKE_FAILURE_THRESHOLD,
) -> tuple[str, int | None]:
    consecutive_failures = 0
    next_check_at = time.monotonic() + max(0.0, check_interval_seconds)

    while True:
        return_code = session.proc.poll()
        if return_code is not None:
            return "process_exit", return_code

        now = time.monotonic()
        if now < next_check_at:
            time.sleep(min(DEFAULT_PROCESS_POLL_INTERVAL_SECONDS, next_check_at - now))
            continue

        smoke_ok, smoke_details = run_smoke_through_proxy(
            listen_port,
            target_url=healthcheck_url,
            proxy_scheme=DEFAULT_PROXY_SCHEME,
        )
        if smoke_ok:
            if consecutive_failures > 0:
                logger.info(
                    "xray_runtime_smoke_recovered",
                    "Active Xray route recovered after transient runtime failures.",
                    {
                        "listen_port": listen_port,
                        "active_link_role": session.candidate_role,
                        "recovered_after_failures": consecutive_failures,
                        "target_url": healthcheck_url,
                    },
                )
            consecutive_failures = 0
            next_check_at = time.monotonic() + max(0.0, check_interval_seconds)
            continue

        consecutive_failures += 1
        logger.warning(
            "xray_runtime_smoke_failed",
            "Active Xray route failed a runtime smoke check.",
            {
                "listen_port": listen_port,
                "active_link_role": session.candidate_role,
                "target_url": healthcheck_url,
                "details": smoke_details,
                "consecutive_failures": consecutive_failures,
                "failure_threshold": failure_threshold,
            },
        )
        if consecutive_failures >= max(1, failure_threshold):
            return "failover", None

        next_check_at = time.monotonic() + max(0.0, check_interval_seconds)


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
    active_session: ActiveProxySession | None = None
    shutdown_requested = False

    def stop_child(signum: int, _frame) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.info(
            "xray_signal_received",
            "Xray client entrypoint received a shutdown signal.",
            {"signal": signum},
        )
        stop_active_session(active_session)

    signal.signal(signal.SIGTERM, stop_child)
    signal.signal(signal.SIGINT, stop_child)
    active_session = activate_candidate(
        candidates=candidates,
        start_index=0,
        listen_port=listen_port,
        log_level=log_level,
        healthcheck_url=healthcheck_url,
        rotation_reason="startup",
    )

    if active_session is None:
        logger.error(
            "xray_all_candidates_failed",
            "All configured VLESS candidates failed to start a working proxy route.",
            {"listen_port": listen_port, "candidate_count": len(candidates)},
        )
        return 1

    while True:
        outcome, return_code = monitor_active_candidate(
            active_session,
            listen_port=listen_port,
            healthcheck_url=healthcheck_url,
        )

        if shutdown_requested and outcome != "process_exit":
            stop_active_session(active_session)
            return 0

        if outcome == "process_exit":
            join_relay_threads(active_session.relay_threads)
            exit_ctx = {
                "return_code": return_code,
                "active_link_role": active_session.candidate_role,
                "link_summary": active_session.link_summary,
            }
            if return_code == 0:
                logger.info("xray_process_exited", "Xray process exited cleanly.", exit_ctx)
            else:
                logger.error("xray_process_exited", "Xray process exited with a non-zero code.", exit_ctx)

            if shutdown_requested:
                return return_code or 0

            active_session = activate_candidate(
                candidates=candidates,
                start_index=active_session.candidate_index + 1,
                listen_port=listen_port,
                log_level=log_level,
                healthcheck_url=healthcheck_url,
                rotation_reason="process_exit",
            )
            if active_session is None:
                logger.error(
                    "xray_all_candidates_failed",
                    "All configured VLESS candidates failed to restore a working proxy route.",
                    {"listen_port": listen_port, "candidate_count": len(candidates)},
                )
                return 1
            continue

        log_candidate_rotation(
            listen_port=listen_port,
            failed_candidate_role=active_session.candidate_role,
            next_candidate_role=candidates[(active_session.candidate_index + 1) % len(candidates)][0],
            reason="runtime_smoke",
        )
        stop_active_session(active_session)
        active_session = activate_candidate(
            candidates=candidates,
            start_index=active_session.candidate_index + 1,
            listen_port=listen_port,
            log_level=log_level,
            healthcheck_url=healthcheck_url,
            rotation_reason="runtime_smoke",
        )
        if active_session is None:
            logger.error(
                "xray_all_candidates_failed",
                "All configured VLESS candidates failed to restore a working proxy route.",
                {"listen_port": listen_port, "candidate_count": len(candidates)},
            )
            return 1


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
