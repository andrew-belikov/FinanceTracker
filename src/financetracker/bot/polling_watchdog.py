"""Stateful Telegram polling watchdog."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PollingWatchdogState:
    backlog_active: bool = False
    detection_streak: int = 0
    self_heal_requested: bool = False
    exit_code: int = 0


async def run_polling_watchdog(
    context,
    *,
    state: PollingWatchdogState,
    now_utc,
    get_last_update_received_at_utc,
    get_process_started_at_utc,
    is_polling_backlog_detected,
    next_polling_backlog_detection_streak,
    should_trigger_polling_self_heal,
    to_iso_datetime,
    pending_threshold: int,
    stall_threshold_seconds: int,
    recovery_confirmation_count: int,
    self_heal_exit_code: int,
    logger,
) -> PollingWatchdogState:
    last_update_received_at = get_last_update_received_at_utc()
    process_started_at = get_process_started_at_utc()
    reference_dt = last_update_received_at or process_started_at
    stall_duration_seconds = int((now_utc - reference_dt).total_seconds())
    try:
        webhook_info = await context.bot.get_webhook_info()
        pending_update_count = int(webhook_info.pending_update_count or 0)
    except Exception:
        logger.exception(
            "bot_polling_watchdog_failed", "Polling watchdog failed to query Telegram webhook state.",
            {"stall_duration_seconds": stall_duration_seconds,
             "last_update_received_at": to_iso_datetime(last_update_received_at),
             "process_started_at": to_iso_datetime(process_started_at)},
        )
        return state

    backlog_detected = is_polling_backlog_detected(
        pending_update_count=pending_update_count,
        last_update_received_at=last_update_received_at,
        process_started_at=process_started_at,
        now_utc=now_utc,
    )
    state.detection_streak = next_polling_backlog_detection_streak(
        backlog_detected=backlog_detected, current_streak=state.detection_streak,
    )
    ctx = {
        "pending_update_count": pending_update_count,
        "stall_duration_seconds": stall_duration_seconds,
        "pending_threshold": pending_threshold,
        "stall_threshold_seconds": stall_threshold_seconds,
        "detection_streak": state.detection_streak,
        "recovery_confirmation_count": recovery_confirmation_count,
        "self_heal_requested": state.self_heal_requested,
        "self_heal_exit_code": self_heal_exit_code,
        "last_update_received_at": to_iso_datetime(last_update_received_at),
        "process_started_at": to_iso_datetime(process_started_at),
    }
    if backlog_detected and not state.backlog_active:
        state.backlog_active = True
        logger.error("bot_polling_backlog_detected", "Telegram polling appears stalled: updates are accumulating.", ctx)
    if not state.self_heal_requested and should_trigger_polling_self_heal(
        backlog_detected=backlog_detected, detection_streak=state.detection_streak,
    ):
        state.self_heal_requested = True
        state.exit_code = self_heal_exit_code
        ctx["self_heal_requested"] = True
        logger.critical(
            "bot_polling_self_heal_triggered",
            "Confirmed Telegram polling stall. Stopping bot process for automatic restart.", ctx,
        )
        context.application.stop_running()
        return state
    if not backlog_detected and state.backlog_active:
        state.backlog_active = False
        state.self_heal_requested = False
        logger.info("bot_polling_backlog_cleared", "Telegram polling backlog cleared.", ctx)
    return state
