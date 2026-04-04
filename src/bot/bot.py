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

# =============== HELPERS ==================


# =============== HANDLERS =================


# ============ DAILY JOB (JOBQUEUE) ========


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
