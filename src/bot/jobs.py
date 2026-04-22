from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from telegram.ext import ContextTypes

from queries import (
    claim_daily_job_run,
    complete_daily_job_run,
    get_pending_invest_notifications,
    get_unnotified_income_events,
    mark_income_event_notified,
    mark_invest_notification_sent,
    release_daily_job_run,
    resolve_reporting_account_id,
)
from report_client import ReporterClientError, request_monthly_report_pdf
from runtime import (
    DAILY_JOB_HOUR,
    DAILY_JOB_SCHEDULE_LABEL,
    DAILY_JOB_MINUTE,
    POLLING_BACKLOG_PENDING_THRESHOLD,
    POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
    POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
    POLLING_SELF_HEAL_EXIT_CODE,
    TARGET_CHAT_IDS,
    TZ,
    YESTERDAY_PEAK_ALERT_HOUR,
    YESTERDAY_PEAK_ALERT_MINUTE,
    YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL,
    db_session,
    decimal_to_str,
    fmt_decimal_rub,
    fmt_plain_pct,
    fmt_signed_amount,
    get_process_started_at_utc,
    get_last_update_received_at_utc,
    is_polling_backlog_detected,
    last_day_of_month,
    logger,
    next_polling_backlog_detection_streak,
    normalize_decimal,
    safe_send_document,
    safe_send_message,
    should_trigger_polling_self_heal,
    to_iso_datetime,
)
from services import (
    build_invest_text_for_account,
    build_month_summary,
    build_triggers_messages,
    build_week_summary,
    build_yesterday_peak_alert_message,
)


POLLING_BACKLOG_ACTIVE = False
POLLING_BACKLOG_DETECTION_STREAK = 0
POLLING_SELF_HEAL_REQUESTED = False
BOT_EXIT_CODE = 0
DAILY_JOB_NAME = "daily_summary"
MONTHLY_PDF_JOB_NAME = "monthly_pdf_delivery"
YESTERDAY_PEAK_ALERT_JOB_NAME = "yesterday_peak_alert"
DAILY_JOB_STARTUP_CATCHUP_DELAY_SECONDS = 5
YESTERDAY_PEAK_ALERT_STARTUP_CATCHUP_DELAY_SECONDS = 7


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


def should_release_daily_job_run(sent_total: int, failed_total: int) -> bool:
    return sent_total == 0 and failed_total > 0


def _claim_scheduled_job_run(
    *,
    job_name: str,
    run_date,
    trigger_source: str,
    scheduled_for: str,
) -> tuple[bool, bool]:
    with db_session() as session:
        run_claimed = claim_daily_job_run(session, job_name=job_name, run_date=run_date)

    if run_claimed is False:
        logger.info(
            "daily_job_already_processed",
            "Daily job already processed for this date; skipping duplicate run.",
            {
                "today": run_date.isoformat(),
                "scheduled_for": scheduled_for,
                "trigger_source": trigger_source,
                "job_name": job_name,
            },
        )
        return False, True

    if run_claimed is None:
        logger.warning(
            "daily_job_tracking_unavailable",
            "Daily job run tracking is unavailable because migration is not applied.",
            {
                "today": run_date.isoformat(),
                "trigger_source": trigger_source,
                "job_name": job_name,
            },
        )
        return True, False

    return True, True


def _finalize_scheduled_job_run(
    *,
    tracking_available: bool,
    job_name: str,
    run_date,
    trigger_source: str,
    sent_total: int,
    failed_total: int,
) -> None:
    if not tracking_available:
        return

    with db_session() as session:
        if should_release_daily_job_run(sent_total=sent_total, failed_total=failed_total):
            release_daily_job_run(session, job_name=job_name, run_date=run_date)
            logger.warning(
                "daily_job_run_released_for_retry",
                "Released daily job claim because all sends failed.",
                {
                    "today": run_date.isoformat(),
                    "trigger_source": trigger_source,
                    "sent_total": sent_total,
                    "failed_total": failed_total,
                    "job_name": job_name,
                },
            )
        else:
            complete_daily_job_run(
                session,
                job_name=job_name,
                run_date=run_date,
                sent_total=sent_total,
                failed_total=failed_total,
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

    now_utc = datetime.now(timezone.utc)
    last_update_received_at = get_last_update_received_at_utc()
    process_started_at = get_process_started_at_utc()
    reference_dt = last_update_received_at or process_started_at
    stall_duration_seconds = int((now_utc - reference_dt).total_seconds())

    try:
        webhook_info = await context.bot.get_webhook_info()
        pending_update_count = int(webhook_info.pending_update_count or 0)
    except Exception:
        logger.exception(
            "bot_polling_watchdog_failed",
            "Polling watchdog failed to query Telegram webhook state.",
            {
                "stall_duration_seconds": stall_duration_seconds,
                "last_update_received_at": to_iso_datetime(last_update_received_at),
                "process_started_at": to_iso_datetime(process_started_at),
            },
        )
        return

    backlog_detected = is_polling_backlog_detected(
        pending_update_count=pending_update_count,
        last_update_received_at=last_update_received_at,
        process_started_at=process_started_at,
        now_utc=now_utc,
    )
    POLLING_BACKLOG_DETECTION_STREAK = next_polling_backlog_detection_streak(
        backlog_detected=backlog_detected,
        current_streak=POLLING_BACKLOG_DETECTION_STREAK,
    )
    ctx = {
        "pending_update_count": pending_update_count,
        "stall_duration_seconds": stall_duration_seconds,
        "pending_threshold": POLLING_BACKLOG_PENDING_THRESHOLD,
        "stall_threshold_seconds": POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
        "detection_streak": POLLING_BACKLOG_DETECTION_STREAK,
        "recovery_confirmation_count": POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
        "self_heal_requested": POLLING_SELF_HEAL_REQUESTED,
        "self_heal_exit_code": POLLING_SELF_HEAL_EXIT_CODE,
        "last_update_received_at": to_iso_datetime(last_update_received_at),
        "process_started_at": to_iso_datetime(process_started_at),
    }

    if backlog_detected and not POLLING_BACKLOG_ACTIVE:
        POLLING_BACKLOG_ACTIVE = True
        logger.error(
            "bot_polling_backlog_detected",
            "Telegram polling appears stalled: updates are accumulating.",
            ctx,
        )

    if (
        not POLLING_SELF_HEAL_REQUESTED
        and should_trigger_polling_self_heal(
            backlog_detected=backlog_detected,
            detection_streak=POLLING_BACKLOG_DETECTION_STREAK,
        )
    ):
        POLLING_SELF_HEAL_REQUESTED = True
        BOT_EXIT_CODE = POLLING_SELF_HEAL_EXIT_CODE
        ctx["self_heal_requested"] = True
        logger.critical(
            "bot_polling_self_heal_triggered",
            "Confirmed Telegram polling stall. Stopping bot process for automatic restart.",
            ctx,
        )
        context.application.stop_running()
        return

    if not backlog_detected and POLLING_BACKLOG_ACTIVE:
        POLLING_BACKLOG_ACTIVE = False
        POLLING_SELF_HEAL_REQUESTED = False
        logger.info(
            "bot_polling_backlog_cleared",
            "Telegram polling backlog cleared.",
            ctx,
        )


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
    should_run, tracking_available = _claim_scheduled_job_run(
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
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, message, parse_mode="Markdown")
                sent_total += 1
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
    daily_should_run, daily_tracking_available = _claim_scheduled_job_run(
        job_name=DAILY_JOB_NAME,
        run_date=today,
        trigger_source=trigger_source,
        scheduled_for=scheduled_for,
    )
    month_pdf_should_run = False
    month_pdf_tracking_available = False
    if is_month_end:
        month_pdf_should_run, month_pdf_tracking_available = _claim_scheduled_job_run(
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

    try:
        for chat_id in TARGET_CHAT_IDS:
            # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
            if daily_should_run and is_month_end:
                if month_text:
                    try:
                        await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
                        daily_sent_total += 1
                        sent_total += 1
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
                        await safe_send_document(
                            context.bot,
                            chat_id,
                            file_path=month_pdf_path,
                            filename=month_pdf_filename,
                            caption="PDF-версия месячного отчёта.",
                        )
                        month_sent_total += 1
                        sent_total += 1
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
                    await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
                    daily_sent_total += 1
                    sent_total += 1
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
                for msg in triggers:
                    try:
                        await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
                        daily_sent_total += 1
                        sent_total += 1
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
            job_name=DAILY_JOB_NAME,
            run_date=today,
            trigger_source=trigger_source,
            sent_total=daily_sent_total,
            failed_total=daily_failed_total,
        )
    if month_pdf_should_run:
        _finalize_scheduled_job_run(
            tracking_available=month_pdf_tracking_available,
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


async def check_income_events(context: ContextTypes.DEFAULT_TYPE):
    rows: list[dict] = []
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return

        rows = get_unnotified_income_events(session, account_id)

    for row in rows:
        event_type = row["event_type"]
        icon = "💸" if event_type == "coupon" else "💰"
        action_line = "Купон зачислен" if event_type == "coupon" else "Дивиденды зачислены"
        net_amount = float(row["net_amount"])
        net_yield_pct = float(row["net_yield_pct"])
        instrument_name = row["instrument_name"]

        text_msg = (
            f"{icon} {instrument_name}\n"
            f"{action_line}\n"
            f"{fmt_signed_amount(net_amount)} ₽ ({fmt_plain_pct(net_yield_pct)} %)"
        )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "income_event_notification_sent",
                    "Income event notification sent.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "income_event_notification_failed",
                    "Failed to send income event notification.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            mark_income_event_notified(session, row["id"])


async def check_invest_notifications(context: ContextTypes.DEFAULT_TYPE):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return
        rows = get_pending_invest_notifications(session, account_id)

    if rows is None:
        return

    for row in rows:
        amount = normalize_decimal(row["amount"])
        with db_session() as session:
            text_msg = build_invest_text_for_account(
                session,
                account_id,
                amount,
                header=f"💸 Получено пополнение: {fmt_decimal_rub(amount, precision=0)}",
            )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "invest_notification_sent",
                    "Invest notification sent.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "invest_notification_failed",
                    "Failed to send invest notification.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            marked = mark_invest_notification_sent(
                session,
                account_id=account_id,
                operation_id=row["operation_id"],
                operation_date=row["date"],
                amount=amount,
            )
        if not marked:
            return
