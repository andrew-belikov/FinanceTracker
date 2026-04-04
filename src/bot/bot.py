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

from datetime import time

from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
)

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
    jobqueue_smoke_test_job,
)
from runtime import (
    HOST_TZ,
    JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
    JOBQUEUE_SMOKE_TEST_ON_START,
    TARGET_CHAT_IDS,
    TELEGRAM_BOT_TOKEN,
    logger,
)

# ==========================================

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

    job_time = time(
        hour=18,
        minute=0,
        tzinfo=HOST_TZ,
    )
    job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")
    job_queue.run_repeating(check_invest_notifications, interval=60, first=15, name="invest_notifier")

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
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    register_handlers(app)
    configure_jobs(app)
    return app


def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "missing_telegram_bot_token",
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную.",
        )
        return 1

    app = build_application()

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
