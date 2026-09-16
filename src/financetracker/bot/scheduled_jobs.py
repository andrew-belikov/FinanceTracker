from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from telegram.ext import ContextTypes

from financetracker.common.finance import annualize_simple_yield_pct
from financetracker.bot.notification_jobs import (
    build_income_event_notification_text as build_income_event_notification_text_impl,
)
from financetracker.bot.notification_delivery import (
    notification_deliveries_are_complete,
    send_tracked_notification,
)
from financetracker.bot.notification_repository import (
    claim_daily_job_run,
    claim_notification_delivery,
    complete_daily_job_run,
    complete_notification_delivery,
    get_notification_delivery_status,
    heartbeat_daily_job_run,
    mark_notification_delivery_uncertain,
    notification_deliveries_complete,
    release_daily_job_run,
    release_notification_delivery,
)
from financetracker.bot.alerts_service import (
    build_triggers_messages,
    build_yesterday_peak_alert_message,
)
from financetracker.bot.payout_service import build_payout_calendar_text_for_account
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.polling_watchdog import (
    PollingWatchdogState,
    run_polling_watchdog,
)
from financetracker.bot.scheduled_job_runs import (
    claim_scheduled_job_run,
    finalize_scheduled_job_run,
    heartbeat_scheduled_job_run,
)
from financetracker.bot.report_client import ReporterClientError, request_monthly_report_pdf
from financetracker.bot.runtime import (
    DAILY_JOB_HOUR,
    DAILY_JOB_SCHEDULE_LABEL,
    DAILY_JOB_MINUTE,
    POLLING_BACKLOG_PENDING_THRESHOLD,
    POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
    POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
    POLLING_SELF_HEAL_EXIT_CODE,
    PAYOUT_WEEKLY_HOUR,
    PAYOUT_WEEKLY_MINUTE,
    PAYOUT_WEEKLY_SCHEDULE_LABEL,
    PAYOUT_WEEKLY_TZ,
    TARGET_CHAT_IDS,
    TZ,
    YESTERDAY_PEAK_ALERT_HOUR,
    YESTERDAY_PEAK_ALERT_MINUTE,
    YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL,
    db_session,
    fmt_plain_pct,
    fmt_signed_amount,
    get_process_started_at_utc,
    get_last_update_received_at_utc,
    is_polling_backlog_detected,
    last_day_of_month,
    logger,
    next_polling_backlog_detection_streak,
    safe_send_document,
    safe_send_message,
    should_trigger_polling_self_heal,
    to_iso_datetime,
)
from financetracker.bot.summary_service import (
    build_month_summary,
    build_week_summary,
)


POLLING_BACKLOG_ACTIVE = False
POLLING_BACKLOG_DETECTION_STREAK = 0
POLLING_SELF_HEAL_REQUESTED = False
BOT_EXIT_CODE = 0
DAILY_JOB_NAME = "daily_summary"
MONTHLY_PDF_JOB_NAME = "monthly_pdf_delivery"
YESTERDAY_PEAK_ALERT_JOB_NAME = "yesterday_peak_alert"
PAYOUT_WEEKLY_JOB_NAME = "weekly_payout_digest"
DAILY_JOB_STARTUP_CATCHUP_DELAY_SECONDS = 5
YESTERDAY_PEAK_ALERT_STARTUP_CATCHUP_DELAY_SECONDS = 7
PAYOUT_WEEKLY_STARTUP_CATCHUP_DELAY_SECONDS = 9


def build_income_event_notification_text(row: dict) -> str:
    return build_income_event_notification_text_impl(
        row,
        annualize_simple_yield_pct=annualize_simple_yield_pct,
        fmt_signed_amount=fmt_signed_amount,
        fmt_plain_pct=fmt_plain_pct,
    )


def reset_polling_watchdog_state() -> None:
    global BOT_EXIT_CODE
    global POLLING_BACKLOG_ACTIVE
    global POLLING_BACKLOG_DETECTION_STREAK
    global POLLING_SELF_HEAL_REQUESTED

    BOT_EXIT_CODE = 0
    POLLING_BACKLOG_ACTIVE = False
    POLLING_BACKLOG_DETECTION_STREAK = 0
    POLLING_SELF_HEAL_REQUESTED = False


def get_bot_exit_code() -> int:
    return BOT_EXIT_CODE


def is_daily_job_catchup_due(now_local: datetime) -> bool:
    scheduled_at = now_local.replace(
        hour=DAILY_JOB_HOUR,
        minute=DAILY_JOB_MINUTE,
        second=0,
        microsecond=0,
    )
    return now_local >= scheduled_at


def is_yesterday_peak_alert_catchup_due(now_local: datetime) -> bool:
    scheduled_at = now_local.replace(
        hour=YESTERDAY_PEAK_ALERT_HOUR,
        minute=YESTERDAY_PEAK_ALERT_MINUTE,
        second=0,
        microsecond=0,
    )
    return now_local >= scheduled_at


def is_payout_weekly_catchup_due(now_local: datetime) -> bool:
    if now_local.weekday() != 0:
        return False
    scheduled_at = now_local.replace(
        hour=PAYOUT_WEEKLY_HOUR,
        minute=PAYOUT_WEEKLY_MINUTE,
        second=0,
        microsecond=0,
    )
    return now_local >= scheduled_at


def should_release_daily_job_run(sent_total: int, failed_total: int) -> bool:
    return sent_total == 0 and failed_total > 0


def _claim_scheduled_job_run(
    *,
    job_name: str,
    run_date,
    trigger_source: str,
    scheduled_for: str,
) -> tuple[bool, bool, str | None]:
    return claim_scheduled_job_run(
        db_session=db_session, claim_run=claim_daily_job_run, logger=logger,
        job_name=job_name, run_date=run_date, trigger_source=trigger_source,
        scheduled_for=scheduled_for,
    )


def _heartbeat_scheduled_job_run(
    *,
    tracking_available: bool,
    job_name: str,
    run_date,
    attempt_id: str | None,
) -> bool:
    return heartbeat_scheduled_job_run(
        db_session=db_session, heartbeat_run=heartbeat_daily_job_run,
        tracking_available=tracking_available, job_name=job_name, run_date=run_date,
        attempt_id=attempt_id,
    )


def _finalize_scheduled_job_run(
    *,
    tracking_available: bool,
    attempt_id: str | None,
    job_name: str,
    run_date,
    trigger_source: str,
    sent_total: int,
    failed_total: int,
) -> None:
    finalize_scheduled_job_run(
        db_session=db_session, complete_run=complete_daily_job_run,
        release_run=release_daily_job_run, logger=logger,
        tracking_available=tracking_available, attempt_id=attempt_id,
        job_name=job_name, run_date=run_date, trigger_source=trigger_source,
        sent_total=sent_total, failed_total=failed_total,
    )


async def _send_tracked_notification(
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    send,
    reclaim_stale: bool = True,
) -> bool:
    return await send_tracked_notification(
        db_session=db_session, claim_delivery=claim_notification_delivery,
        complete_delivery=complete_notification_delivery,
        get_delivery_status=get_notification_delivery_status,
        mark_uncertain=mark_notification_delivery_uncertain,
        release_delivery=release_notification_delivery,
        notification_kind=notification_kind, notification_key=notification_key,
        chat_id=chat_id, message_type=message_type, send=send,
        reclaim_stale=reclaim_stale,
    )


def _all_notification_deliveries_complete(
    *,
    notification_kind: str,
    notification_key: str,
    message_types: set[str],
) -> bool:
    return notification_deliveries_are_complete(
        db_session=db_session, deliveries_complete=notification_deliveries_complete,
        target_chat_ids=TARGET_CHAT_IDS, notification_kind=notification_kind,
        notification_key=notification_key, message_types=message_types,
    )


async def jobqueue_smoke_test_job(context: ContextTypes.DEFAULT_TYPE):
    sent = 0
    failed = 0
    now_local = datetime.now(TZ)
    text_msg = (
        "🧪 JobQueue smoke-test\n"
        f"Время (локальное): {now_local.strftime('%d.%m.%Y %H:%M:%S %Z')}\n"
        "Отправка из одноразового тестового джоба при старте."
    )

    for chat_id in TARGET_CHAT_IDS:
        try:
            await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
            logger.exception(
                "jobqueue_smoke_test_failed",
                "JobQueue smoke-test failed.",
                {"chat_id": chat_id},
            )

    logger.info(
        "jobqueue_smoke_test_completed",
        "JobQueue smoke-test completed.",
        {
            "sent": sent,
            "failed": failed,
            "target_chat_ids": sorted(TARGET_CHAT_IDS),
        },
    )


async def polling_watchdog_job(context: ContextTypes.DEFAULT_TYPE):
    global BOT_EXIT_CODE
    global POLLING_BACKLOG_ACTIVE
    global POLLING_BACKLOG_DETECTION_STREAK
    global POLLING_SELF_HEAL_REQUESTED

    state = await run_polling_watchdog(
        context,
        state=PollingWatchdogState(
            backlog_active=POLLING_BACKLOG_ACTIVE,
            detection_streak=POLLING_BACKLOG_DETECTION_STREAK,
            self_heal_requested=POLLING_SELF_HEAL_REQUESTED,
            exit_code=BOT_EXIT_CODE,
        ),
        now_utc=datetime.now(timezone.utc),
        get_last_update_received_at_utc=get_last_update_received_at_utc,
        get_process_started_at_utc=get_process_started_at_utc,
        is_polling_backlog_detected=is_polling_backlog_detected,
        next_polling_backlog_detection_streak=next_polling_backlog_detection_streak,
        should_trigger_polling_self_heal=should_trigger_polling_self_heal,
        to_iso_datetime=to_iso_datetime,
        pending_threshold=POLLING_BACKLOG_PENDING_THRESHOLD,
        stall_threshold_seconds=POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
        recovery_confirmation_count=POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
        self_heal_exit_code=POLLING_SELF_HEAL_EXIT_CODE,
        logger=logger,
    )
    POLLING_BACKLOG_ACTIVE = state.backlog_active
    POLLING_BACKLOG_DETECTION_STREAK = state.detection_streak
    POLLING_SELF_HEAL_REQUESTED = state.self_heal_requested
    BOT_EXIT_CODE = state.exit_code


async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_daily_job(context, trigger_source="scheduled")


async def daily_job_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(TZ)
    if not is_daily_job_catchup_due(now_local):
        logger.info(
            "daily_job_catchup_not_due",
            "Skipping startup catch-up because daily job time has not been reached yet.",
            {
                "today": now_local.date().isoformat(),
                "scheduled_for": DAILY_JOB_SCHEDULE_LABEL,
                "started_at": now_local.isoformat(),
            },
        )
        return

    await _run_daily_job(context, trigger_source="startup_catchup", now_local=now_local)


async def yesterday_peak_alert_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_yesterday_peak_alert_job(context, trigger_source="scheduled")


async def yesterday_peak_alert_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(TZ)
    if not is_yesterday_peak_alert_catchup_due(now_local):
        logger.info(
            "yesterday_peak_alert_catchup_not_due",
            "Skipping startup catch-up because yesterday peak alert time has not been reached yet.",
            {
                "today": now_local.date().isoformat(),
                "scheduled_for": YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL,
                "started_at": now_local.isoformat(),
            },
        )
        return

    await _run_yesterday_peak_alert_job(context, trigger_source="startup_catchup", now_local=now_local)


async def payout_weekly_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_payout_weekly_job(context, trigger_source="scheduled")


async def payout_weekly_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(PAYOUT_WEEKLY_TZ)
    if not is_payout_weekly_catchup_due(now_local):
        logger.info(
            "payout_weekly_catchup_not_due",
            "Skipping startup catch-up because the weekly payout digest is not due.",
            {
                "today": now_local.date().isoformat(),
                "scheduled_for": PAYOUT_WEEKLY_SCHEDULE_LABEL,
                "started_at": now_local.isoformat(),
            },
        )
        return

    await _run_payout_weekly_job(
        context,
        trigger_source="startup_catchup",
        now_local=now_local,
    )


async def _run_payout_weekly_job(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    trigger_source: str,
    now_local: datetime | None = None,
):
    now_local = now_local or datetime.now(PAYOUT_WEEKLY_TZ)
    week_start = now_local.date()
    week_end = week_start + timedelta(days=6)
    should_run, tracking_available, run_attempt_id = _claim_scheduled_job_run(
        job_name=PAYOUT_WEEKLY_JOB_NAME,
        run_date=week_start,
        trigger_source=trigger_source,
        scheduled_for=PAYOUT_WEEKLY_SCHEDULE_LABEL,
    )
    if not should_run:
        return

    logger.info(
        "payout_weekly_started",
        "Weekly payout digest started.",
        {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "scheduled_for": PAYOUT_WEEKLY_SCHEDULE_LABEL,
            "trigger_source": trigger_source,
            "tracking_available": tracking_available,
        },
    )

    message = None
    sent_total = 0
    failed_total = 0
    try:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if account_id is None:
                raise RuntimeError("Reporting account is unavailable")
            message = build_payout_calendar_text_for_account(
                session,
                account_id,
                start_date=week_start,
                end_date=week_end,
                heading="💸 Выплаты на этой неделе",
            )
    except Exception:
        failed_total = len(TARGET_CHAT_IDS) or 1
        logger.exception(
            "payout_weekly_build_failed",
            "Failed to build weekly payout digest.",
            {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "trigger_source": trigger_source,
            },
        )

    if message:
        if not _heartbeat_scheduled_job_run(
            tracking_available=tracking_available,
            job_name=PAYOUT_WEEKLY_JOB_NAME,
            run_date=week_start,
            attempt_id=run_attempt_id,
        ):
            return
        for chat_id in TARGET_CHAT_IDS:
            if not _heartbeat_scheduled_job_run(
                tracking_available=tracking_available,
                job_name=PAYOUT_WEEKLY_JOB_NAME,
                run_date=week_start,
                attempt_id=run_attempt_id,
            ):
                return
            try:
                delivered_now = await _send_tracked_notification(
                    notification_kind="scheduled_job",
                    notification_key=f"{PAYOUT_WEEKLY_JOB_NAME}:{week_start.isoformat()}",
                    chat_id=chat_id,
                    message_type="weekly_payout_digest",
                    send=lambda chat_id=chat_id: safe_send_message(
                        context.bot,
                        chat_id,
                        message,
                        parse_mode=None,
                    ),
                )
                sent_total += int(delivered_now)
                if delivered_now:
                    logger.info(
                        "payout_weekly_sent",
                        "Weekly payout digest sent.",
                        {
                            "chat_id": chat_id,
                            "week_start": week_start.isoformat(),
                            "week_end": week_end.isoformat(),
                        },
                    )
            except Exception:
                failed_total += 1
                logger.exception(
                    "payout_weekly_send_failed",
                    "Failed to send weekly payout digest.",
                    {
                        "chat_id": chat_id,
                        "week_start": week_start.isoformat(),
                        "week_end": week_end.isoformat(),
                    },
                )

    _finalize_scheduled_job_run(
        tracking_available=tracking_available,
        attempt_id=run_attempt_id,
        job_name=PAYOUT_WEEKLY_JOB_NAME,
        run_date=week_start,
        trigger_source=trigger_source,
        sent_total=sent_total,
        failed_total=failed_total,
    )
    logger.info(
        "payout_weekly_completed",
        "Weekly payout digest completed.",
        {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "sent_total": sent_total,
            "failed_total": failed_total,
            "message_ready": bool(message),
            "trigger_source": trigger_source,
            "tracking_available": tracking_available,
        },
    )


async def _run_yesterday_peak_alert_job(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    trigger_source: str,
    now_local: datetime | None = None,
):
    now_local = now_local or datetime.now(TZ)
    target_date = now_local.date() - timedelta(days=1)
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    should_run, tracking_available, run_attempt_id = _claim_scheduled_job_run(
        job_name=YESTERDAY_PEAK_ALERT_JOB_NAME,
        run_date=target_date,
        trigger_source=trigger_source,
        scheduled_for=YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL,
    )
    if not should_run:
        return

    logger.info(
        "yesterday_peak_alert_started",
        "Yesterday peak alert started.",
        {
            "today": now_local.date().isoformat(),
            "target_date": target_date.isoformat(),
            "scheduled_for": YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL,
            "started_at": started_at.isoformat(),
            "trigger_source": trigger_source,
            "tracking_available": tracking_available,
        },
    )

    sent_total = 0
    failed_total = 0
    message: str | None = None
    try:
        message = build_yesterday_peak_alert_message(now_local=now_local)
    except Exception:
        failed_total = len(TARGET_CHAT_IDS) or 1
        logger.exception(
            "yesterday_peak_alert_build_failed",
            "Failed to build yesterday peak alert.",
            {"target_date": target_date.isoformat(), "trigger_source": trigger_source},
        )

    if message:
        if not _heartbeat_scheduled_job_run(
            tracking_available=tracking_available,
            job_name=YESTERDAY_PEAK_ALERT_JOB_NAME,
            run_date=target_date,
            attempt_id=run_attempt_id,
        ):
            return
        for chat_id in TARGET_CHAT_IDS:
            if not _heartbeat_scheduled_job_run(
                tracking_available=tracking_available,
                job_name=YESTERDAY_PEAK_ALERT_JOB_NAME,
                run_date=target_date,
                attempt_id=run_attempt_id,
            ):
                return
            try:
                delivered_now = await _send_tracked_notification(
                    notification_kind="scheduled_job",
                    notification_key=(
                        f"{YESTERDAY_PEAK_ALERT_JOB_NAME}:{target_date.isoformat()}"
                    ),
                    chat_id=chat_id,
                    message_type="yesterday_peak_alert",
                    send=lambda chat_id=chat_id: safe_send_message(
                        context.bot,
                        chat_id,
                        message,
                        parse_mode="Markdown",
                    ),
                )
                sent_total += int(delivered_now)
                if delivered_now:
                    logger.info(
                        "yesterday_peak_alert_sent",
                        "Yesterday peak alert sent.",
                        {"chat_id": chat_id, "target_date": target_date.isoformat()},
                    )
            except Exception:
                failed_total += 1
                logger.exception(
                    "yesterday_peak_alert_send_failed",
                    "Failed to send yesterday peak alert.",
                    {"chat_id": chat_id, "target_date": target_date.isoformat()},
                )

    _finalize_scheduled_job_run(
        tracking_available=tracking_available,
        attempt_id=run_attempt_id,
        job_name=YESTERDAY_PEAK_ALERT_JOB_NAME,
        run_date=target_date,
        trigger_source=trigger_source,
        sent_total=sent_total,
        failed_total=failed_total,
    )

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    logger.info(
        "yesterday_peak_alert_completed",
        "Yesterday peak alert completed.",
        {
            "target_date": target_date.isoformat(),
            "duration_ms": duration_ms,
            "sent_total": sent_total,
            "failed_total": failed_total,
            "message_ready": bool(message),
            "trigger_source": trigger_source,
            "tracking_available": tracking_available,
        },
    )


async def _run_daily_job(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    trigger_source: str,
    now_local: datetime | None = None,
):
    """
    Авто-рассылки по расписанию (по TIMEZONE):
    - каждый день в заданное время: проверка годового плана
    - каждую пятницу в заданное время: недельный отчёт (/week)
    - в последний день месяца в заданное время: месячный отчёт (/month) и PDF-версия

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = now_local or datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = DAILY_JOB_SCHEDULE_LABEL
    daily_should_run, daily_tracking_available, daily_attempt_id = _claim_scheduled_job_run(
        job_name=DAILY_JOB_NAME,
        run_date=today,
        trigger_source=trigger_source,
        scheduled_for=scheduled_for,
    )
    month_pdf_should_run = False
    month_pdf_tracking_available = False
    month_pdf_attempt_id = None
    if is_month_end:
        month_pdf_should_run, month_pdf_tracking_available, month_pdf_attempt_id = _claim_scheduled_job_run(
            job_name=MONTHLY_PDF_JOB_NAME,
            run_date=today,
            trigger_source=trigger_source,
            scheduled_for=scheduled_for,
        )

    if not daily_should_run and not month_pdf_should_run:
        return

    logger.info(
        "daily_job_started",
        "Daily job started.",
        {
            "today": today.isoformat(),
            "scheduled_for": scheduled_for,
            "started_at": started_at.isoformat(),
            "is_month_end": is_month_end,
            "is_friday": is_friday,
            "trigger_source": trigger_source,
            "tracking_available": daily_tracking_available or month_pdf_tracking_available,
            "daily_should_run": daily_should_run,
            "month_pdf_should_run": month_pdf_should_run,
        },
    )

    month_text = None
    month_pdf_path = None
    month_pdf_filename = None
    week_text = None
    triggers: list[str] = []

    try:
        if daily_should_run and is_month_end:
            month_text = build_month_summary()
    except Exception:
        logger.exception("daily_job_month_summary_failed", "Failed to build month summary.")

    try:
        if daily_should_run and is_friday:
            week_text = build_week_summary()
    except Exception:
        logger.exception("daily_job_week_summary_failed", "Failed to build week summary.")

    try:
        if daily_should_run:
            triggers = build_triggers_messages()
    except Exception:
        logger.exception("daily_job_triggers_failed", "Failed to build trigger messages.")

    if month_pdf_should_run:
        try:
            month_pdf_path, month_pdf_filename = await asyncio.to_thread(
                request_monthly_report_pdf,
                year=today.year,
                month=today.month,
            )
        except ReporterClientError as exc:
            logger.warning(
                "daily_job_monthly_pdf_failed",
                "Failed to fetch monthly PDF report for daily job.",
                {
                    "today": today.isoformat(),
                    "error": str(exc),
                    "trigger_source": trigger_source,
                },
            )
        except Exception:
            logger.exception(
                "daily_job_monthly_pdf_failed",
                "Failed to fetch monthly PDF report for daily job.",
                {
                    "today": today.isoformat(),
                    "trigger_source": trigger_source,
                },
            )

    logger.info(
        "daily_job_messages_prepared",
        "Daily job prepared messages.",
        {
            "month_report_ready": bool(month_text),
            "month_pdf_ready": bool(month_pdf_path),
            "week_report_ready": bool(week_text),
            "triggers_count": len(triggers),
            "trigger_source": trigger_source,
        },
    )

    # Нечего отправлять — выходим тихо.
    if not month_pdf_should_run and not month_text and not week_text and not triggers:
        _finalize_scheduled_job_run(
            tracking_available=daily_tracking_available,
            attempt_id=daily_attempt_id,
            job_name=DAILY_JOB_NAME,
            run_date=today,
            trigger_source=trigger_source,
            sent_total=0,
            failed_total=0,
        )
        logger.info(
            "daily_job_no_messages",
            "Daily job had no messages to send.",
            {"today": today.isoformat(), "trigger_source": trigger_source},
        )
        return

    sent_total = 0
    failed_total = 0
    daily_sent_total = 0
    daily_failed_total = 0
    month_sent_total = 0
    month_failed_total = 0

    if daily_should_run and not _heartbeat_scheduled_job_run(
        tracking_available=daily_tracking_available,
        job_name=DAILY_JOB_NAME,
        run_date=today,
        attempt_id=daily_attempt_id,
    ):
        daily_should_run = False
    if month_pdf_should_run and not _heartbeat_scheduled_job_run(
        tracking_available=month_pdf_tracking_available,
        job_name=MONTHLY_PDF_JOB_NAME,
        run_date=today,
        attempt_id=month_pdf_attempt_id,
    ):
        month_pdf_should_run = False
    if not daily_should_run and not month_pdf_should_run:
        if month_pdf_path and os.path.exists(month_pdf_path):
            os.remove(month_pdf_path)
        return

    try:
        for chat_id in TARGET_CHAT_IDS:
            if daily_should_run and not _heartbeat_scheduled_job_run(
                tracking_available=daily_tracking_available,
                job_name=DAILY_JOB_NAME,
                run_date=today,
                attempt_id=daily_attempt_id,
            ):
                daily_should_run = False
            if month_pdf_should_run and not _heartbeat_scheduled_job_run(
                tracking_available=month_pdf_tracking_available,
                job_name=MONTHLY_PDF_JOB_NAME,
                run_date=today,
                attempt_id=month_pdf_attempt_id,
            ):
                month_pdf_should_run = False
            if not daily_should_run and not month_pdf_should_run:
                break
            # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
            if daily_should_run and is_month_end:
                if month_text:
                    try:
                        delivered_now = await _send_tracked_notification(
                            notification_kind="scheduled_job",
                            notification_key=f"{DAILY_JOB_NAME}:{today.isoformat()}",
                            chat_id=chat_id,
                            message_type="month_report",
                            send=lambda chat_id=chat_id: safe_send_message(
                                context.bot,
                                chat_id,
                                month_text,
                                parse_mode="Markdown",
                            ),
                        )
                        daily_sent_total += int(delivered_now)
                        sent_total += int(delivered_now)
                        if delivered_now:
                            logger.info(
                                "daily_job_message_sent",
                                "Daily job message sent.",
                                {"chat_id": chat_id, "message_type": "month_report"},
                            )
                    except Exception:
                        daily_failed_total += 1
                        failed_total += 1
                        logger.exception(
                            "daily_job_message_send_failed",
                            "Failed to send daily job month report.",
                            {"chat_id": chat_id, "message_type": "month_report"},
                        )
                else:
                    daily_failed_total += 1
                    failed_total += 1
                    logger.warning(
                        "daily_job_month_report_unavailable",
                        "Monthly text report is unavailable for daily job delivery.",
                        {
                            "chat_id": chat_id,
                            "trigger_source": trigger_source,
                            "today": today.isoformat(),
                        },
                    )

            if month_pdf_should_run:
                if month_pdf_path and month_pdf_filename:
                    try:
                        delivered_now = await _send_tracked_notification(
                            notification_kind="scheduled_job",
                            notification_key=f"{MONTHLY_PDF_JOB_NAME}:{today.isoformat()}",
                            chat_id=chat_id,
                            message_type="month_pdf",
                            send=lambda chat_id=chat_id: safe_send_document(
                                context.bot,
                                chat_id,
                                file_path=month_pdf_path,
                                filename=month_pdf_filename,
                                caption="PDF-версия месячного отчёта.",
                            ),
                        )
                        month_sent_total += int(delivered_now)
                        sent_total += int(delivered_now)
                        if delivered_now:
                            logger.info(
                                "daily_job_message_sent",
                                "Daily job message sent.",
                                {"chat_id": chat_id, "message_type": "month_pdf"},
                            )
                    except Exception:
                        month_failed_total += 1
                        failed_total += 1
                        logger.exception(
                            "daily_job_message_send_failed",
                            "Failed to send daily job monthly PDF report.",
                            {"chat_id": chat_id, "message_type": "month_pdf"},
                        )
                else:
                    month_failed_total += 1
                    failed_total += 1
                    logger.warning(
                        "daily_job_month_delivery_unavailable",
                        "Monthly PDF delivery is unavailable.",
                        {
                            "chat_id": chat_id,
                            "trigger_source": trigger_source,
                            "today": today.isoformat(),
                        },
                    )

            if daily_should_run and is_friday and week_text:
                try:
                    delivered_now = await _send_tracked_notification(
                        notification_kind="scheduled_job",
                        notification_key=f"{DAILY_JOB_NAME}:{today.isoformat()}",
                        chat_id=chat_id,
                        message_type="week_report",
                        send=lambda chat_id=chat_id: safe_send_message(
                            context.bot,
                            chat_id,
                            week_text,
                            parse_mode="Markdown",
                        ),
                    )
                    daily_sent_total += int(delivered_now)
                    sent_total += int(delivered_now)
                    if delivered_now:
                        logger.info(
                            "daily_job_message_sent",
                            "Daily job message sent.",
                            {"chat_id": chat_id, "message_type": "week_report"},
                        )
                except Exception:
                    daily_failed_total += 1
                    failed_total += 1
                    logger.exception(
                        "daily_job_message_send_failed",
                        "Failed to send daily job week report.",
                        {"chat_id": chat_id, "message_type": "week_report"},
                    )

            if daily_should_run:
                for trigger_index, msg in enumerate(triggers):
                    try:
                        delivered_now = await _send_tracked_notification(
                            notification_kind="scheduled_job",
                            notification_key=f"{DAILY_JOB_NAME}:{today.isoformat()}",
                            chat_id=chat_id,
                            message_type=f"trigger:{trigger_index}",
                            send=lambda chat_id=chat_id, msg=msg: safe_send_message(
                                context.bot,
                                chat_id,
                                msg,
                                parse_mode="Markdown",
                            ),
                        )
                        daily_sent_total += int(delivered_now)
                        sent_total += int(delivered_now)
                        if delivered_now:
                            logger.info(
                                "daily_job_message_sent",
                                "Daily job message sent.",
                                {"chat_id": chat_id, "message_type": "trigger"},
                            )
                    except Exception:
                        daily_failed_total += 1
                        failed_total += 1
                        logger.exception(
                            "daily_job_message_send_failed",
                            "Failed to send daily job trigger message.",
                            {"chat_id": chat_id, "message_type": "trigger"},
                        )
    finally:
        if month_pdf_path and os.path.exists(month_pdf_path):
            os.remove(month_pdf_path)

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    if daily_should_run:
        _finalize_scheduled_job_run(
            tracking_available=daily_tracking_available,
            attempt_id=daily_attempt_id,
            job_name=DAILY_JOB_NAME,
            run_date=today,
            trigger_source=trigger_source,
            sent_total=daily_sent_total,
            failed_total=daily_failed_total,
        )
    if month_pdf_should_run:
        _finalize_scheduled_job_run(
            tracking_available=month_pdf_tracking_available,
            attempt_id=month_pdf_attempt_id,
            job_name=MONTHLY_PDF_JOB_NAME,
            run_date=today,
            trigger_source=trigger_source,
            sent_total=month_sent_total,
            failed_total=month_failed_total,
        )
    logger.info(
        "daily_job_completed",
        "Daily job completed.",
        {
            "today": today.isoformat(),
            "duration_ms": duration_ms,
            "sent_total": sent_total,
            "failed_total": failed_total,
            "daily_sent_total": daily_sent_total,
            "daily_failed_total": daily_failed_total,
            "month_sent_total": month_sent_total,
            "month_failed_total": month_failed_total,
            "trigger_source": trigger_source,
            "tracking_available": daily_tracking_available or month_pdf_tracking_available,
        },
    )
