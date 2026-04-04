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

- Ежедневная задача (в заданное время JobQueue по TIMEZONE):
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

from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from handlers import (
    cmd_dataset,
    cmd_help,
    cmd_history,
    cmd_invest,
    cmd_month,
    cmd_rebalance,
    cmd_start,
    cmd_structure,
    cmd_targets,
    cmd_today,
    cmd_twr,
    cmd_week,
    cmd_year,
    debug_command_probe,
)
from jobs import (
    check_income_events,
    check_invest_notifications,
    daily_job,
    get_bot_exit_code,
    jobqueue_smoke_test_job,
    polling_watchdog_job,
    reset_polling_watchdog_state,
)
from runtime import (
    BOT_PROXY_ENABLED,
    BOT_PROXY_ENDPOINT,
    DAILY_JOB_SCHEDULE_LABEL,
    JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
    JOBQUEUE_SMOKE_TEST_ON_START,
    POLLING_WATCHDOG_INTERVAL_SECONDS,
    TARGET_CHAT_IDS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE,
    TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT_SECONDS,
    TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS,
    TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS,
    TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS,
    TELEGRAM_GET_UPDATES_WRITE_TIMEOUT_SECONDS,
    TELEGRAM_POLL_INTERVAL_SECONDS,
    TELEGRAM_REQUEST_CONNECTION_POOL_SIZE,
    TELEGRAM_REQUEST_CONNECT_TIMEOUT_SECONDS,
    TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS,
    TELEGRAM_REQUEST_READ_TIMEOUT_SECONDS,
    TELEGRAM_REQUEST_WRITE_TIMEOUT_SECONDS,
    TZ_NAME,
    build_daily_job_time,
    build_telegram_request_kwargs,
    logger,
    reset_update_tracking_state,
    resolve_telegram_proxy_url,
)


COMMAND_HANDLERS = (
    ("start", cmd_start),
    ("help", cmd_help),
    ("today", cmd_today),
    ("week", cmd_week),
    ("month", cmd_month),
    ("year", cmd_year),
    ("dataset", cmd_dataset),
    ("structure", cmd_structure),
    ("history", cmd_history),
    ("twr", cmd_twr),
    ("targets", cmd_targets),
    ("rebalance", cmd_rebalance),
    ("invest", cmd_invest),
)

BOT_STARTUP_RETRY_EXIT_CODE = 76


def register_handlers(app: Application) -> None:
    app.add_handler(MessageHandler(filters.COMMAND, debug_command_probe), group=-1)
    for command_name, handler in COMMAND_HANDLERS:
        app.add_handler(CommandHandler(command_name, handler))


def configure_jobs(app: Application) -> None:
    job_queue = app.job_queue
    if job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    job_time = build_daily_job_time()
    job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")
    job_queue.run_repeating(check_invest_notifications, interval=60, first=15, name="invest_notifier")
    job_queue.run_repeating(
        polling_watchdog_job,
        interval=POLLING_WATCHDOG_INTERVAL_SECONDS,
        first=POLLING_WATCHDOG_INTERVAL_SECONDS,
        name="polling_watchdog",
    )
    logger.info(
        "bot_jobqueue_jobs_registered",
        "JobQueue jobs registered.",
        {
            "daily_job_schedule": DAILY_JOB_SCHEDULE_LABEL,
            "schedule_timezone": TZ_NAME,
            "target_chat_ids": sorted(TARGET_CHAT_IDS),
            "income_events_interval_seconds": 60,
            "invest_interval_seconds": 60,
            "polling_watchdog_interval_seconds": POLLING_WATCHDOG_INTERVAL_SECONDS,
        },
    )

    if JOBQUEUE_SMOKE_TEST_ON_START:
        job_queue.run_once(
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


def build_application() -> Application:
    proxy_url = resolve_telegram_proxy_url()
    request = HTTPXRequest(
        **build_telegram_request_kwargs(
            proxy_url=proxy_url,
            connection_pool_size=TELEGRAM_REQUEST_CONNECTION_POOL_SIZE,
            connect_timeout=TELEGRAM_REQUEST_CONNECT_TIMEOUT_SECONDS,
            read_timeout=TELEGRAM_REQUEST_READ_TIMEOUT_SECONDS,
            write_timeout=TELEGRAM_REQUEST_WRITE_TIMEOUT_SECONDS,
            pool_timeout=TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS,
        )
    )
    get_updates_request = HTTPXRequest(
        **build_telegram_request_kwargs(
            proxy_url=proxy_url,
            connection_pool_size=TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE,
            connect_timeout=TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT_SECONDS,
            read_timeout=TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS,
            write_timeout=TELEGRAM_GET_UPDATES_WRITE_TIMEOUT_SECONDS,
            pool_timeout=TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS,
        )
    )
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .build()
    )
    register_handlers(app)
    app.add_error_handler(on_application_error)
    configure_jobs(app)
    return app


async def on_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = getattr(context, "error", None)
    update_id = getattr(update, "update_id", None)
    user_id = getattr(getattr(update, "effective_user", None), "id", None)
    chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    ctx = {
        "update_id": update_id,
        "user_id": user_id,
        "chat_id": chat_id,
        "error_type": type(err).__name__ if err is not None else None,
        "error_message": str(err) if err is not None else None,
    }
    if err is not None:
        logger.raw_logger.error(
            "Unhandled Telegram application error.",
            extra={"event": "bot_application_error", "ctx": ctx},
            exc_info=(type(err), err, err.__traceback__),
        )
        return
    logger.error(
        "bot_application_error",
        "Unhandled Telegram application error without exception object.",
        ctx,
    )


def is_retryable_telegram_transport_error(exc: Exception) -> bool:
    return isinstance(exc, (TimedOut, NetworkError))


def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "missing_telegram_bot_token",
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную.",
        )
        return 1

    reset_update_tracking_state()
    reset_polling_watchdog_state()
    app = build_application()

    logger.info(
        "bot_telegram_transport_configured",
        "Configured Telegram transport for polling and bot API requests.",
        {
            "proxy_enabled": BOT_PROXY_ENABLED,
            "proxy_url": BOT_PROXY_ENDPOINT if BOT_PROXY_ENABLED else None,
            "request_pool_size": TELEGRAM_REQUEST_CONNECTION_POOL_SIZE,
            "request_pool_timeout_seconds": TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS,
            "get_updates_pool_size": TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE,
            "get_updates_pool_timeout_seconds": TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS,
            "get_updates_timeout_seconds": TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS,
            "get_updates_read_timeout_seconds": TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS,
        },
    )
    logger.info(
        "bot_started",
        "Bot started.",
        {
            "daily_job_schedule": DAILY_JOB_SCHEDULE_LABEL,
            "schedule_timezone": TZ_NAME,
        },
    )
    try:
        app.run_polling(
            timeout=TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS,
            poll_interval=TELEGRAM_POLL_INTERVAL_SECONDS,
        )
    except Exception as exc:
        if not is_retryable_telegram_transport_error(exc):
            raise
        logger.exception(
            "bot_telegram_transport_failed",
            "Telegram transport failed while initializing or polling; requesting supervised restart.",
            {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "retry_exit_code": BOT_STARTUP_RETRY_EXIT_CODE,
                "proxy_enabled": BOT_PROXY_ENABLED,
                "proxy_url": BOT_PROXY_ENDPOINT if BOT_PROXY_ENABLED else None,
            },
        )
        return BOT_STARTUP_RETRY_EXIT_CODE
    return get_bot_exit_code()


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
