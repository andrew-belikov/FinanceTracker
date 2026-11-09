"""Explicit operator replay from retained observations, without polling or writes."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path
import tempfile

from financetracker.bot.daily_report_repository import report_bounds
from financetracker.bot.charts import build_intraday_daily_push_chart
from financetracker.bot.intraday_daily_push import build_intraday_daily_push_payload_from_day
from financetracker.bot.intraday_daily_push_job import _cash_events
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import TARGET_CHAT_IDS, TZ, db_session, safe_send_message, safe_send_photo
from financetracker.database.intraday import get_intraday_day


async def replay_intraday_daily_push(report_date: date, *, send: bool = False) -> int:
    """Render once; send only on request, leaving scheduled delivery state intact."""
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError("Не найден счёт для отчёта.")
        day = get_intraday_day(session, account_id, report_date)
        if len(day.snapshots) < 2:
            raise ValueError("Нет минутных данных для повторной отправки выбранного дня.")
        start, end = report_bounds(day)
        payload = build_intraday_daily_push_payload_from_day(
            day=day, local_tz=TZ,
            cash_events=_cash_events(session, account_id, report_date, day.snapshots[-1].currency, start=start, end=end),
        )
    if payload is None:
        raise ValueError("Недостаточно наблюдений со стоимостью портфеля.")
    recipients = sorted(TARGET_CHAT_IDS)
    if send and not recipients:
        raise ValueError("Не настроены получатели рассылки.")

    with tempfile.TemporaryDirectory(prefix="financetracker-intraday-replay-") as directory:
        path = str(Path(directory) / "intraday.png")
        result = await asyncio.to_thread(
            build_intraday_daily_push_chart, path, report_date=report_date,
            snapshots=payload["snapshots"], positive_movers=payload["positive_movers"],
            negative_movers=payload["negative_movers"], change_components=payload["components"],
        )
        if result is None:
            raise ValueError("Не удалось подготовить график.")
        if not send:
            return 0

        # Initializing Bot alone does not start polling or scheduled jobs.
        from financetracker.bot.bot import build_application

        async with build_application().bot as bot:
            for chat_id in recipients:
                await safe_send_message(bot, chat_id, payload["text"], parse_mode=None)
                await safe_send_photo(bot, chat_id, file_path=path, filename=f"intraday-{report_date.isoformat()}.png")
    return len(recipients)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Проверка или повторная отправка сохранённого внутридневного итога.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="Дата в формате YYYY-MM-DD")
    parser.add_argument("--send", action="store_true", help="Отправить текст и график всем TARGET_CHAT_IDS")
    args = parser.parse_args(argv)
    try:
        sent = asyncio.run(replay_intraday_daily_push(args.date, send=args.send))
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")
    print(f"Отправлено получателям: {sent}." if args.send else "Текст и график подготовлены. Отправка не выполнялась.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
