from datetime import datetime, timezone

from telegram.ext import ContextTypes

from queries import (
    get_pending_invest_notifications,
    get_unnotified_income_events,
    mark_income_event_notified,
    mark_invest_notification_sent,
    resolve_reporting_account_id,
)
from runtime import (
    DAILY_JOB_SCHEDULE_LABEL,
    POLLING_BACKLOG_PENDING_THRESHOLD,
    POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
    POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
    POLLING_SELF_HEAL_EXIT_CODE,
    TARGET_CHAT_IDS,
    TZ,
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
    safe_send_message,
    should_trigger_polling_self_heal,
    to_iso_datetime,
)
from services import (
    build_invest_text_for_account,
    build_month_summary,
    build_triggers_messages,
    build_week_summary,
)


POLLING_BACKLOG_ACTIVE = False
POLLING_BACKLOG_DETECTION_STREAK = 0
POLLING_SELF_HEAL_REQUESTED = False
BOT_EXIT_CODE = 0


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
    """
    Авто-рассылки по расписанию (по TIMEZONE):
    - каждый день в заданное время: проверка триггеров (новый максимум / годовой план)
    - каждую пятницу в заданное время: недельный отчёт (/week)
    - в последний день месяца в заданное время: месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = DAILY_JOB_SCHEDULE_LABEL

    logger.info(
        "daily_job_started",
        "Daily job started.",
        {
            "today": today.isoformat(),
            "scheduled_for": scheduled_for,
            "started_at": started_at.isoformat(),
            "is_month_end": is_month_end,
            "is_friday": is_friday,
        },
    )

    month_text = None
    week_text = None
    triggers: list[str] = []

    try:
        if is_month_end:
            month_text = build_month_summary()
    except Exception:
        logger.exception("daily_job_month_summary_failed", "Failed to build month summary.")

    try:
        if is_friday:
            week_text = build_week_summary()
    except Exception:
        logger.exception("daily_job_week_summary_failed", "Failed to build week summary.")

    try:
        triggers = build_triggers_messages()
    except Exception:
        logger.exception("daily_job_triggers_failed", "Failed to build trigger messages.")

    logger.info(
        "daily_job_messages_prepared",
        "Daily job prepared messages.",
        {
            "month_report_ready": bool(month_text),
            "week_report_ready": bool(week_text),
            "triggers_count": len(triggers),
        },
    )

    # Нечего отправлять — выходим тихо.
    if not month_text and not week_text and not triggers:
        logger.info(
            "daily_job_no_messages",
            "Daily job had no messages to send.",
            {"today": today.isoformat()},
        )
        return

    sent_total = 0
    failed_total = 0

    for chat_id in TARGET_CHAT_IDS:
        # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
        if is_month_end and month_text:
            try:
                await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job month report.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )

        if is_friday and week_text:
            try:
                await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job week report.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )

        for msg in triggers:
            try:
                await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job trigger message.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    logger.info(
        "daily_job_completed",
        "Daily job completed.",
        {
            "today": today.isoformat(),
            "duration_ms": duration_ms,
            "sent_total": sent_total,
            "failed_total": failed_total,
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
