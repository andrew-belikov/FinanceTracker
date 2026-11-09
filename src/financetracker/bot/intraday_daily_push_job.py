"""Weekday intraday digest scheduler."""
from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime

from financetracker.bot.charts import build_intraday_daily_push_chart
from financetracker.bot.daily_report_repository import get_daily_report_events, report_bounds
from financetracker.bot.intraday_daily_push import build_intraday_daily_push_payload_from_day
from financetracker.bot.notification_delivery import notification_deliveries_are_complete, send_tracked_notification
from financetracker.bot.notification_repository import claim_daily_job_run, claim_notification_delivery, complete_daily_job_run, complete_notification_delivery, get_notification_delivery_status, mark_notification_delivery_uncertain, notification_deliveries_complete, release_daily_job_run, release_notification_delivery
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import INTRADAY_DAILY_PUSH_HOUR, INTRADAY_DAILY_PUSH_MINUTE, INTRADAY_DAILY_PUSH_SCHEDULE_LABEL, TARGET_CHAT_IDS, TZ, db_session, logger, safe_send_message, safe_send_photo
from financetracker.bot.scheduled_jobs import claim_scheduled_job_run, finalize_scheduled_job_run
from financetracker.database.intraday import get_intraday_day, mark_intraday_daily_summary_delivered, upsert_intraday_daily_summary

INTRADAY_DAILY_PUSH_JOB_NAME = "intraday_daily_push"
INTRADAY_DAILY_PUSH_STARTUP_CATCHUP_DELAY_SECONDS = 11


def is_intraday_daily_push_catchup_due(now_local):
    return now_local.weekday() < 5 and now_local >= now_local.replace(hour=INTRADAY_DAILY_PUSH_HOUR, minute=INTRADAY_DAILY_PUSH_MINUTE, second=0, microsecond=0)


def _cash_events(session, account_id, day, currency, *, start=None, end=None):
    return get_daily_report_events(session, account_id, day, currency, start=start, end=end)


async def intraday_daily_push_job(context):
    await _run(context, "scheduled")


async def intraday_daily_push_startup_catchup(context):
    now = datetime.now(TZ)
    if is_intraday_daily_push_catchup_due(now):
        await _run(context, "startup_catchup", now)


async def _run(context, source, now=None):
    now = now or datetime.now(TZ)
    day = now.date()
    if day.weekday() > 4:
        return
    run, tracked, attempt = claim_scheduled_job_run(db_session=db_session, claim_run=claim_daily_job_run, logger=logger, job_name=INTRADAY_DAILY_PUSH_JOB_NAME, run_date=day, trigger_source=source, scheduled_for=INTRADAY_DAILY_PUSH_SCHEDULE_LABEL)
    if not run:
        return
    sent = failed = 0
    path = account_id = None
    try:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if not account_id:
                failed = 1
                return
            intraday = get_intraday_day(session, account_id, day)
            if len(intraday.snapshots) < 2:
                return
            start, end = report_bounds(intraday)
            payload = build_intraday_daily_push_payload_from_day(
                day=intraday, local_tz=TZ,
                cash_events=_cash_events(session, account_id, day, intraday.snapshots[-1].currency, start=start, end=end))
            if payload is None:
                return
            upsert_intraday_daily_summary(session, intraday)
            session.commit()
        with tempfile.NamedTemporaryFile(prefix="financetracker-intraday-", suffix=".png", delete=False) as chart:
            path = chart.name
        await asyncio.to_thread(build_intraday_daily_push_chart, path, report_date=day, snapshots=payload["snapshots"], positive_movers=payload["positive_movers"], negative_movers=payload["negative_movers"], change_components=payload["components"])
        key = f"{INTRADAY_DAILY_PUSH_JOB_NAME}:{day.isoformat()}"
        for chat_id in TARGET_CHAT_IDS:
            for typ, send in (("intraday_daily_push_text", lambda: safe_send_message(context.bot, chat_id, payload["text"], parse_mode=None)), ("intraday_daily_push_chart", lambda: safe_send_photo(context.bot, chat_id, file_path=path, filename=f"intraday-{day.isoformat()}.png"))):
                try:
                    sent += int(await send_tracked_notification(db_session=db_session, claim_delivery=claim_notification_delivery, complete_delivery=complete_notification_delivery, get_delivery_status=get_notification_delivery_status, mark_uncertain=mark_notification_delivery_uncertain, release_delivery=release_notification_delivery, notification_kind="scheduled_job", notification_key=key, chat_id=chat_id, message_type=typ, send=send))
                except Exception:
                    failed += 1
                    logger.exception("intraday_daily_push_delivery_failed", "Failed to deliver intraday daily push.", {"chat_id": chat_id, "message_type": typ, "report_date": day.isoformat()})
        if notification_deliveries_are_complete(db_session=db_session, deliveries_complete=notification_deliveries_complete, target_chat_ids=TARGET_CHAT_IDS, notification_kind="scheduled_job", notification_key=key, message_types={"intraday_daily_push_text", "intraday_daily_push_chart"}):
            with db_session() as session:
                mark_intraday_daily_summary_delivered(session, account_id, day)
                session.commit()
    except Exception:
        failed += 1
        logger.exception("intraday_daily_push_failed", "Failed to prepare or deliver intraday daily push.", {"report_date": day.isoformat(), "trigger_source": source})
    finally:
        if path and os.path.exists(path):
            os.remove(path)
        finalize_scheduled_job_run(db_session=db_session, complete_run=complete_daily_job_run, release_run=release_daily_job_run, logger=logger, tracking_available=tracked, attempt_id=attempt, job_name=INTRADAY_DAILY_PUSH_JOB_NAME, run_date=day, trigger_source=source, sent_total=sent, failed_total=failed)
