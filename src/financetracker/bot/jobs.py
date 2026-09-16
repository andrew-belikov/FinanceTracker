"""Public facade for bot jobs; scheduled workflow implementation is separate."""
from __future__ import annotations

from datetime import datetime, timezone
from telegram.ext import ContextTypes

from financetracker.common.finance import annualize_simple_yield_pct
from financetracker.bot import scheduled_jobs as _scheduled_jobs
from financetracker.bot.iis_tax_deduction import build_iis_tax_deduction_markup
from financetracker.bot.notification_delivery import notification_deliveries_are_complete, send_tracked_notification
from financetracker.bot.notification_jobs import build_income_event_notification_text as build_income_event_notification_text_impl, check_income_events as check_income_events_impl, check_invest_notifications as check_invest_notifications_impl
from financetracker.bot.notification_repository import claim_daily_job_run, claim_notification_delivery, complete_daily_job_run, complete_notification_delivery, get_notification_delivery_status, get_pending_invest_notifications, get_unnotified_income_events, heartbeat_daily_job_run, mark_income_event_notified, mark_invest_notification_sent, mark_notification_delivery_uncertain, notification_deliveries_complete, release_daily_job_run, release_notification_delivery
from financetracker.bot.polling_watchdog import PollingWatchdogState, run_polling_watchdog
from financetracker.bot import alerts_service as _alerts_service
from financetracker.bot.payout_service import build_payout_calendar_text_for_account  # noqa: F401 - public monkeypatch seam
from financetracker.bot.report_client import request_monthly_report_pdf  # noqa: F401 - public monkeypatch seam
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.summary_service import build_month_summary, build_week_summary  # noqa: F401 - public monkeypatch seams
from financetracker.bot.runtime import DAILY_JOB_HOUR, DAILY_JOB_MINUTE, DAILY_JOB_SCHEDULE_LABEL, IIS_TAX_DEDUCTION_CATEGORY, PAYOUT_WEEKLY_HOUR, PAYOUT_WEEKLY_MINUTE, PAYOUT_WEEKLY_SCHEDULE_LABEL, PAYOUT_WEEKLY_TZ, POLLING_BACKLOG_PENDING_THRESHOLD, POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT, POLLING_BACKLOG_STALL_THRESHOLD_SECONDS, POLLING_SELF_HEAL_EXIT_CODE, TARGET_CHAT_IDS, TZ, YESTERDAY_PEAK_ALERT_HOUR, YESTERDAY_PEAK_ALERT_MINUTE, YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL, db_session, decimal_to_str, fmt_decimal_rub, fmt_plain_pct, fmt_signed_amount, get_last_update_received_at_utc, get_process_started_at_utc, is_polling_backlog_detected, logger, next_polling_backlog_detection_streak, normalize_decimal, safe_send_document, safe_send_message, should_trigger_polling_self_heal, to_iso_datetime  # noqa: F401 - public monkeypatch seams
from financetracker.bot.rebalance_service import build_invest_text_for_account

# Public monkeypatch seams preserved for scheduled workflow callers.
build_triggers_messages = _alerts_service.build_triggers_messages
build_yesterday_peak_alert_message = _alerts_service.build_yesterday_peak_alert_message

POLLING_BACKLOG_ACTIVE = False
POLLING_BACKLOG_DETECTION_STREAK = 0
POLLING_SELF_HEAL_REQUESTED = False
BOT_EXIT_CODE = 0
DAILY_JOB_NAME = _scheduled_jobs.DAILY_JOB_NAME
MONTHLY_PDF_JOB_NAME = _scheduled_jobs.MONTHLY_PDF_JOB_NAME
YESTERDAY_PEAK_ALERT_JOB_NAME = _scheduled_jobs.YESTERDAY_PEAK_ALERT_JOB_NAME
PAYOUT_WEEKLY_JOB_NAME = _scheduled_jobs.PAYOUT_WEEKLY_JOB_NAME
DAILY_JOB_STARTUP_CATCHUP_DELAY_SECONDS = _scheduled_jobs.DAILY_JOB_STARTUP_CATCHUP_DELAY_SECONDS
YESTERDAY_PEAK_ALERT_STARTUP_CATCHUP_DELAY_SECONDS = _scheduled_jobs.YESTERDAY_PEAK_ALERT_STARTUP_CATCHUP_DELAY_SECONDS
PAYOUT_WEEKLY_STARTUP_CATCHUP_DELAY_SECONDS = _scheduled_jobs.PAYOUT_WEEKLY_STARTUP_CATCHUP_DELAY_SECONDS

def build_income_event_notification_text(row: dict) -> str:
    return build_income_event_notification_text_impl(row, annualize_simple_yield_pct=annualize_simple_yield_pct, fmt_signed_amount=fmt_signed_amount, fmt_plain_pct=fmt_plain_pct)

def reset_polling_watchdog_state() -> None:
    global BOT_EXIT_CODE, POLLING_BACKLOG_ACTIVE, POLLING_BACKLOG_DETECTION_STREAK, POLLING_SELF_HEAL_REQUESTED
    BOT_EXIT_CODE = 0
    POLLING_BACKLOG_ACTIVE = False
    POLLING_BACKLOG_DETECTION_STREAK = 0
    POLLING_SELF_HEAL_REQUESTED = False

def get_bot_exit_code() -> int:
    return BOT_EXIT_CODE

def is_daily_job_catchup_due(now_local: datetime) -> bool:
    scheduled_at = now_local.replace(hour=DAILY_JOB_HOUR, minute=DAILY_JOB_MINUTE, second=0, microsecond=0)
    return now_local >= scheduled_at

def is_yesterday_peak_alert_catchup_due(now_local: datetime) -> bool:
    scheduled_at = now_local.replace(hour=YESTERDAY_PEAK_ALERT_HOUR, minute=YESTERDAY_PEAK_ALERT_MINUTE, second=0, microsecond=0)
    return now_local >= scheduled_at

def is_payout_weekly_catchup_due(now_local: datetime) -> bool:
    if now_local.weekday() != 0:
        return False
    scheduled_at = now_local.replace(hour=PAYOUT_WEEKLY_HOUR, minute=PAYOUT_WEEKLY_MINUTE, second=0, microsecond=0)
    return now_local >= scheduled_at

def should_release_daily_job_run(sent_total: int, failed_total: int) -> bool:
    return sent_total == 0 and failed_total > 0

def _claim_scheduled_job_run(*, job_name, run_date, trigger_source, scheduled_for):
    return _scheduled_jobs.claim_scheduled_job_run(
        db_session=db_session, claim_run=claim_daily_job_run, logger=logger,
        job_name=job_name, run_date=run_date, trigger_source=trigger_source,
        scheduled_for=scheduled_for,
    )

def _heartbeat_scheduled_job_run(*, tracking_available, job_name, run_date, attempt_id):
    return _scheduled_jobs.heartbeat_scheduled_job_run(
        db_session=db_session, heartbeat_run=heartbeat_daily_job_run,
        tracking_available=tracking_available, job_name=job_name, run_date=run_date,
        attempt_id=attempt_id,
    )

def _finalize_scheduled_job_run(*, tracking_available, attempt_id, job_name, run_date, trigger_source, sent_total, failed_total):
    _scheduled_jobs.finalize_scheduled_job_run(
        db_session=db_session, complete_run=complete_daily_job_run,
        release_run=release_daily_job_run, logger=logger,
        tracking_available=tracking_available, attempt_id=attempt_id,
        job_name=job_name, run_date=run_date, trigger_source=trigger_source,
        sent_total=sent_total, failed_total=failed_total,
    )

async def _send_tracked_notification(*, notification_kind, notification_key, chat_id, message_type, send, reclaim_stale=True):
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

def _all_notification_deliveries_complete(*, notification_kind, notification_key, message_types):
    return notification_deliveries_are_complete(
        db_session=db_session, deliveries_complete=notification_deliveries_complete,
        target_chat_ids=TARGET_CHAT_IDS, notification_kind=notification_kind,
        notification_key=notification_key, message_types=message_types,
    )

# Existing callers and tests patch these names on jobs.  Refresh the extracted
# implementation immediately before dispatch to preserve that public seam.
_SCHEDULED_JOB_SEAMS = ("TARGET_CHAT_IDS", "TZ", "PAYOUT_WEEKLY_TZ", "DAILY_JOB_HOUR", "DAILY_JOB_MINUTE", "YESTERDAY_PEAK_ALERT_HOUR", "YESTERDAY_PEAK_ALERT_MINUTE", "PAYOUT_WEEKLY_HOUR", "PAYOUT_WEEKLY_MINUTE", "DAILY_JOB_SCHEDULE_LABEL", "YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL", "PAYOUT_WEEKLY_SCHEDULE_LABEL", "db_session", "claim_daily_job_run", "complete_daily_job_run", "release_daily_job_run", "heartbeat_daily_job_run", "_claim_scheduled_job_run", "_heartbeat_scheduled_job_run", "_finalize_scheduled_job_run", "_send_tracked_notification", "resolve_reporting_account_id", "build_month_summary", "build_week_summary", "build_triggers_messages", "build_yesterday_peak_alert_message", "build_payout_calendar_text_for_account", "request_monthly_report_pdf", "safe_send_message", "safe_send_document", "logger")

def _sync_scheduled_job_seams() -> None:
    for name in _SCHEDULED_JOB_SEAMS:
        setattr(_scheduled_jobs, name, globals()[name])

async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_daily_job(context, trigger_source="scheduled")

async def daily_job_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(TZ)
    if not is_daily_job_catchup_due(now_local):
        logger.info("daily_job_catchup_not_due", "Skipping startup catch-up because daily job time has not been reached yet.", {"today": now_local.date().isoformat(), "scheduled_for": DAILY_JOB_SCHEDULE_LABEL, "started_at": now_local.isoformat()})
        return
    await _run_daily_job(context, trigger_source="startup_catchup", now_local=now_local)

async def yesterday_peak_alert_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_yesterday_peak_alert_job(context, trigger_source="scheduled")

async def yesterday_peak_alert_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(TZ)
    if not is_yesterday_peak_alert_catchup_due(now_local):
        logger.info("yesterday_peak_alert_catchup_not_due", "Skipping startup catch-up because yesterday peak alert time has not been reached yet.", {"today": now_local.date().isoformat(), "scheduled_for": YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL, "started_at": now_local.isoformat()})
        return
    await _run_yesterday_peak_alert_job(context, trigger_source="startup_catchup", now_local=now_local)

async def payout_weekly_job(context: ContextTypes.DEFAULT_TYPE):
    await _run_payout_weekly_job(context, trigger_source="scheduled")

async def payout_weekly_startup_catchup(context: ContextTypes.DEFAULT_TYPE):
    now_local = datetime.now(PAYOUT_WEEKLY_TZ)
    if not is_payout_weekly_catchup_due(now_local):
        logger.info("payout_weekly_catchup_not_due", "Skipping startup catch-up because the weekly payout digest is not due.", {"today": now_local.date().isoformat(), "scheduled_for": PAYOUT_WEEKLY_SCHEDULE_LABEL, "started_at": now_local.isoformat()})
        return
    await _run_payout_weekly_job(context, trigger_source="startup_catchup", now_local=now_local)

async def _run_daily_job(context: ContextTypes.DEFAULT_TYPE, *, trigger_source: str, now_local: datetime | None = None):
    _sync_scheduled_job_seams()
    await _scheduled_jobs._run_daily_job(context, trigger_source=trigger_source, now_local=now_local)

async def _run_yesterday_peak_alert_job(context: ContextTypes.DEFAULT_TYPE, *, trigger_source: str, now_local: datetime | None = None):
    _sync_scheduled_job_seams()
    await _scheduled_jobs._run_yesterday_peak_alert_job(context, trigger_source=trigger_source, now_local=now_local)

async def _run_payout_weekly_job(context: ContextTypes.DEFAULT_TYPE, *, trigger_source: str, now_local: datetime | None = None):
    _sync_scheduled_job_seams()
    await _scheduled_jobs._run_payout_weekly_job(context, trigger_source=trigger_source, now_local=now_local)

async def jobqueue_smoke_test_job(context: ContextTypes.DEFAULT_TYPE):
    _sync_scheduled_job_seams()
    await _scheduled_jobs.jobqueue_smoke_test_job(context)

async def polling_watchdog_job(context: ContextTypes.DEFAULT_TYPE):
    global BOT_EXIT_CODE, POLLING_BACKLOG_ACTIVE, POLLING_BACKLOG_DETECTION_STREAK, POLLING_SELF_HEAL_REQUESTED
    state = await run_polling_watchdog(context, state=PollingWatchdogState(backlog_active=POLLING_BACKLOG_ACTIVE, detection_streak=POLLING_BACKLOG_DETECTION_STREAK, self_heal_requested=POLLING_SELF_HEAL_REQUESTED, exit_code=BOT_EXIT_CODE), now_utc=datetime.now(timezone.utc), get_last_update_received_at_utc=get_last_update_received_at_utc, get_process_started_at_utc=get_process_started_at_utc, is_polling_backlog_detected=is_polling_backlog_detected, next_polling_backlog_detection_streak=next_polling_backlog_detection_streak, should_trigger_polling_self_heal=should_trigger_polling_self_heal, to_iso_datetime=to_iso_datetime, pending_threshold=POLLING_BACKLOG_PENDING_THRESHOLD, stall_threshold_seconds=POLLING_BACKLOG_STALL_THRESHOLD_SECONDS, recovery_confirmation_count=POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT, self_heal_exit_code=POLLING_SELF_HEAL_EXIT_CODE, logger=logger)
    POLLING_BACKLOG_ACTIVE, POLLING_BACKLOG_DETECTION_STREAK = state.backlog_active, state.detection_streak
    POLLING_SELF_HEAL_REQUESTED, BOT_EXIT_CODE = state.self_heal_requested, state.exit_code

async def check_income_events(context: ContextTypes.DEFAULT_TYPE):
    await check_income_events_impl(context, db_session=db_session, resolve_reporting_account_id=resolve_reporting_account_id, get_unnotified_income_events=get_unnotified_income_events, target_chat_ids=TARGET_CHAT_IDS, build_notification_text=build_income_event_notification_text, send_tracked_notification=_send_tracked_notification, safe_send_message=safe_send_message, deliveries_complete=_all_notification_deliveries_complete, mark_income_event_notified=mark_income_event_notified, logger=logger)

async def check_invest_notifications(context: ContextTypes.DEFAULT_TYPE):
    await check_invest_notifications_impl(context, db_session=db_session, resolve_reporting_account_id=resolve_reporting_account_id, get_pending_invest_notifications=get_pending_invest_notifications, target_chat_ids=TARGET_CHAT_IDS, normalize_decimal=normalize_decimal, build_invest_text_for_account=build_invest_text_for_account, fmt_decimal_rub=fmt_decimal_rub, build_iis_tax_deduction_markup=build_iis_tax_deduction_markup, iis_tax_deduction_category=IIS_TAX_DEDUCTION_CATEGORY, send_tracked_notification=_send_tracked_notification, safe_send_message=safe_send_message, deliveries_complete=_all_notification_deliveries_complete, mark_invest_notification_sent=mark_invest_notification_sent, decimal_to_str=decimal_to_str, logger=logger)
