"""
Telegram-бот для проекта iis_tracker.

Функции:
- Команды:
    /today      — сводка по портфелю "Семейный капитал" на сегодня
    /week       — сводка по текущей неделе
    /month      — отчёт по текущему месяцу
    /year       — отчёт за год (YTD или календарный)
    /dataset    — архив json+csv+md для AI-анализа
    /structure  — текущая структура портфеля
    /history    — график стоимости портфеля и суммы пополнений
    /twr        — TWR, XIRR и run-rate на конец года + график по дням
    /help       — список команд

- Ежедневная задача (18:00 по времени хоста, через JobQueue):
    * по пятницам — недельный отчёт (/week)
    * в последний день месяца — отчёт за месяц (/month)
    * триггеры:
        - новый максимум портфеля
        - годовой план по пополнениям выполнен (400k за год)
    * (ежедневная сводка /today автоматически НЕ отправляется)

Безопасность:
- ALLOWED_USER_IDS — белый список Telegram user_id.
- Все остальные пользователи игнорируются.
"""

from datetime import datetime, time, timezone

from telegram import Update, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from charts import (
    build_history_chart,
    build_year_chart,
    build_year_monthly_delta_chart,
    render_twr_chart,
)
from dataset import create_dataset_archive
from queries import (
    get_pending_invest_notifications,
    get_unnotified_income_events,
    mark_income_event_notified,
    mark_invest_notification_sent,
    resolve_reporting_account_id,
)
from runtime import (
    HOST_TZ,
    INVEST_USAGE_TEXT,
    JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
    JOBQUEUE_SMOKE_TEST_ON_START,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TARGETS_USAGE_TEXT,
    TARGET_CHAT_IDS,
    TELEGRAM_BOT_TOKEN,
    TZ,
    db_session,
    fmt_decimal_rub,
    fmt_plain_pct,
    fmt_signed_amount,
    is_authorized,
    last_day_of_month,
    log_update_received,
    logger,
    normalize_decimal,
    safe_send_message,
)
from services import (
    build_help_text,
    build_invest_text_for_account,
    build_month_summary,
    build_rebalance_text_for_account,
    build_structure_text,
    build_targets_text_for_account,
    build_today_summary,
    build_triggers_messages,
    build_week_summary,
    build_year_summary,
    compute_portfolio_xirr_and_run_rate,
    compute_twr_timeseries,
    get_year_period,
    parse_rebalance_targets_args,
    render_twr_summary_text,
    replace_rebalance_targets,
)

# ==========================================

# =============== HELPERS ==================


async def debug_command_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_name = None
    text = getattr(update.effective_message, "text", None) or ""
    if text.startswith("/"):
        command_name = text.split()[0]
    log_update_received(update, command_name=command_name)


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


# =============== HANDLERS =================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/start")
    if not is_authorized(update):
        return
    text = (
        "Привет! Я слежу за вашим портфелем «Семейный капитал».\n\n"
        "Доступные команды можно посмотреть в /help."
    )
    logger.info(
        "bot_reply_text_started",
        "Sending reply_text response.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )
    await update.message.reply_text(text)
    logger.info(
        "bot_reply_text_succeeded",
        "reply_text response sent.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/help")
    if not is_authorized(update):
        return

    text = build_help_text()
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/today")
    if not is_authorized(update):
        return
    text = build_today_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/week")
    if not is_authorized(update):
        return
    text = build_week_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/month")
    if not is_authorized(update):
        return
    text = build_month_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/year")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) > 1:
        await update.message.reply_text("Формат: /year или /year YYYY")
        return

    year: int | None = None
    if len(args) == 1:
        try:
            parsed_year = int(args[0])
            if parsed_year < 1900 or parsed_year > 2100:
                raise ValueError
            year = parsed_year
        except ValueError:
            await update.message.reply_text("Формат: /year или /year YYYY")
            return

    try:
        summary_text, diff_text, label = build_year_summary(year)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    temp_chart = tempfile.NamedTemporaryFile(prefix=f"year_{chart_year}_", suffix=".png", delete=False)
    chart_path = temp_chart.name
    temp_chart.close()
    try:
        chart = build_year_chart(
            chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(chart_path):
            os.remove(chart_path)
        await update.message.reply_text(str(exc))
        return
    if chart:
        try:
            with open(chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(chart):
                os.remove(chart)
    else:
        await update.message.reply_text(f"Недостаточно данных для графика за {label}.")

    temp_delta_chart = tempfile.NamedTemporaryFile(prefix=f"year_delta_{chart_year}_", suffix=".png", delete=False)
    delta_chart_path = temp_delta_chart.name
    temp_delta_chart.close()
    try:
        delta_chart = build_year_monthly_delta_chart(
            delta_chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(delta_chart_path):
            os.remove(delta_chart_path)
        await update.message.reply_text(str(exc))
        return
    if delta_chart:
        try:
            with open(delta_chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(delta_chart):
                os.remove(delta_chart)

    await safe_send_message(context.bot, update.effective_chat.id, diff_text, parse_mode="Markdown")


async def cmd_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/dataset")
    if not is_authorized(update):
        return

    if context.args:
        await update.message.reply_text("Формат: /dataset")
        return

    try:
        archive_path, archive_name = create_dataset_archive()
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    try:
        with open(archive_path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=archive_name),
                caption="Архив для AI-анализа: JSON, CSV и README с контекстом.",
            )
    finally:
        if os.path.exists(archive_path):
            os.remove(archive_path)


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/structure")
    if not is_authorized(update):
        return
    text = build_structure_text()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/history")
    if not is_authorized(update):
        return

    path = "/tmp/history.png"
    try:
        p = build_history_chart(path)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    if not p:
        await update.message.reply_text(
            "Недостаточно данных для построения графика."
        )
        return

    with open(p, "rb") as f:
        # Caption убран по требованию
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_twr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/twr")
    if not is_authorized(update):
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return

        data = compute_twr_timeseries(session, account_id)
        xirr_value, projected_value, projection_date = compute_portfolio_xirr_and_run_rate(
            session,
            account_id,
        )

    if not data:
        await update.message.reply_text("Недостаточно данных")
        return

    dates, values, twr = data
    last_date = dates[-1]
    last_value = values[-1]
    last_twr_pct = twr[-1] * 100.0
    summary_text = render_twr_summary_text(
        last_date=last_date,
        last_value=last_value,
        last_twr_pct=last_twr_pct,
        xirr_value=xirr_value,
        projected_value=projected_value,
        projection_date=projection_date,
    )
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    path = "/tmp/twr.png"
    render_twr_chart(path, dates, values, twr)

    with open(path, "rb") as f:
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/targets")
    if not is_authorized(update):
        return

    args = context.args or []
    if not args:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if account_id is None:
                await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
                return
            text = build_targets_text_for_account(session, account_id)
        await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
        return

    if args[0].lower() != "set":
        await update.message.reply_text(TARGETS_USAGE_TEXT)
        return

    try:
        targets = parse_rebalance_targets_args(args[1:])
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        saved = replace_rebalance_targets(session, account_id, targets)
        if not saved:
            await update.message.reply_text(REBALANCE_FEATURE_UNAVAILABLE_TEXT)
            return

    with db_session() as session:
        text = build_targets_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/rebalance")
    if not is_authorized(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /rebalance")
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_rebalance_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_invest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/invest")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) != 1:
        await update.message.reply_text(INVEST_USAGE_TEXT)
        return

    try:
        amount = parse_decimal_input(args[0], allow_zero=False)
        if quantize_ruble_amount(amount) <= 0:
            raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    rounded_amount = quantize_ruble_amount(amount)
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_invest_text_for_account(
            session,
            account_id,
            rounded_amount,
            header=f"💸 Как распределить пополнение {fmt_decimal_rub(rounded_amount, precision=0)}",
        )
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


# ============ DAILY JOB (JOBQUEUE) ========


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


def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "missing_telegram_bot_token",
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную.",
        )
        return 1

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.COMMAND, debug_command_probe), group=-1)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("month", cmd_month))
    app.add_handler(CommandHandler("year", cmd_year))
    app.add_handler(CommandHandler("dataset", cmd_dataset))
    app.add_handler(CommandHandler("structure", cmd_structure))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("twr", cmd_twr))
    app.add_handler(CommandHandler("targets", cmd_targets))
    app.add_handler(CommandHandler("rebalance", cmd_rebalance))
    app.add_handler(CommandHandler("invest", cmd_invest))

    # Ежедневный джоб
    job_time = time(
        hour=18,
        minute=0,
        tzinfo=HOST_TZ,
    )
    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    app.job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    app.job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")
    app.job_queue.run_repeating(check_invest_notifications, interval=60, first=15, name="invest_notifier")

    if JOBQUEUE_SMOKE_TEST_ON_START:
        app.job_queue.run_once(
            jobqueue_smoke_test_job,
            when=JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
            name="jobqueue_smoke_test",
        )
        logger.info(
            "bot_jobqueue_smoke_scheduled",
            "Scheduled one-time JobQueue smoke-test.",
            {
                "delay_seconds": JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
                "target_chat_ids": sorted(TARGET_CHAT_IDS),
            },
        )

    logger.info(
        "bot_started",
        "Bot started.",
        {
            "daily_job_time_local": "18:00",
            "host_timezone": str(HOST_TZ),
        },
    )
    app.run_polling()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "bot_process_failed",
            "Bot process terminated with an unhandled exception.",
        )
        raise SystemExit(1)
