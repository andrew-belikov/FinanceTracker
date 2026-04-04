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
    HOST_TZ,
    TARGET_CHAT_IDS,
    TZ,
    db_session,
    decimal_to_str,
    fmt_decimal_rub,
    fmt_plain_pct,
    fmt_signed_amount,
    last_day_of_month,
    logger,
    normalize_decimal,
    safe_send_message,
)
from services import (
    build_invest_text_for_account,
    build_month_summary,
    build_triggers_messages,
    build_week_summary,
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


async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Авто-рассылки по расписанию (по времени хоста):
    - каждый день в 18:00 (по времени хоста): проверка триггеров (новый максимум / годовой план)
    - каждую пятницу в 18:00 (по времени хоста): недельный отчёт (/week)
    - в последний день месяца в 18:00 (по времени хоста): месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = f"18:00 {HOST_TZ}"

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
