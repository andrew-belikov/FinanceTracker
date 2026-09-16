"""Tracker composition root and backwards-compatible runtime facade."""

from __future__ import annotations

import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from financetracker.tracker import runtime as _legacy
from financetracker.tracker.container_smoke import (
    ContainerSmokeConfigurationError,
    run_if_requested as run_container_smoke_if_requested,
)

# Explicit runtime aliases keep composition local while __getattr__ preserves
# the established tracker.app compatibility surface for direct imports.
API_SESSION = _legacy.API_SESSION
BASE_URL = _legacy.BASE_URL
API_TOKEN = _legacy.API_TOKEN
ACCOUNT_STATUS = _legacy.ACCOUNT_STATUS
PORTFOLIO_CURRENCY = _legacy.PORTFOLIO_CURRENCY
HTTP_TIMEOUT_SECONDS = _legacy.HTTP_TIMEOUT_SECONDS
HTTP_RETRY_TOTAL = _legacy.HTTP_RETRY_TOTAL
HTTP_BACKOFF_SECONDS = _legacy.HTTP_BACKOFF_SECONDS
HTTP_MAX_BACKOFF_SECONDS = _legacy.HTTP_MAX_BACKOFF_SECONDS
INSTRUMENT_CACHE_TTL_SECONDS = _legacy.INSTRUMENT_CACHE_TTL_SECONDS
INSTRUMENT_CACHE_MAX_ENTRIES = _legacy.INSTRUMENT_CACHE_MAX_ENTRIES
OPERATIONS_MAX_PAGES = _legacy.OPERATIONS_MAX_PAGES
VERIFY_SSL = _legacy.VERIFY_SSL
TINKOFF_ACCOUNT_ID = _legacy.TINKOFF_ACCOUNT_ID
SessionLocal = _legacy.SessionLocal
logger = _legacy.logger
local_today = _legacy.local_today
dt_to_iso_z = _legacy.dt_to_iso_z
get_latest_cost_basis = _legacy.get_latest_cost_basis
init_db = _legacy.init_db
run_snapshot_and_operations_once = _legacy.run_snapshot_and_operations_once
payout_calendar_job_with_retry = _legacy.payout_calendar_job_with_retry
clear_tracker_ready_state = _legacy.clear_tracker_ready_state
write_tracker_ready_state = _legacy.write_tracker_ready_state
EXPLICIT_DB_DSN = _legacy.EXPLICIT_DB_DSN
DB_PASSWORD = _legacy.DB_PASSWORD
APP_ENV = _legacy.APP_ENV
SCHED_TZ = _legacy.SCHED_TZ
LOCAL_TZ = _legacy.LOCAL_TZ
PAYOUT_CALENDAR_HORIZON_DAYS = _legacy.PAYOUT_CALENDAR_HORIZON_DAYS
PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS = _legacy.PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS
PAYOUT_CALENDAR_SYNC_HOUR = _legacy.PAYOUT_CALENDAR_SYNC_HOUR
PAYOUT_CALENDAR_SYNC_MINUTE = _legacy.PAYOUT_CALENDAR_SYNC_MINUTE
SNAPSHOT_MODE = _legacy.SNAPSHOT_MODE
SNAPSHOT_HOUR = _legacy.SNAPSHOT_HOUR
SNAPSHOT_MINUTE = _legacy.SNAPSHOT_MINUTE
SNAPSHOT_INTERVAL_MINUTES = _legacy.SNAPSHOT_INTERVAL_MINUTES
_INSTRUMENT_CACHE = _legacy._INSTRUMENT_CACHE

def __getattr__(name):
    if name == "time":
        import time

        return time
    if name == "requests":
        import requests

        return requests
    return getattr(_legacy, name)

class RuntimeConfigurationError(ValueError):
    """Raised when required runtime configuration is absent or unsafe."""


def parse_verify_ssl(raw_value, *, app_env, allow_insecure_test):
    normalized = "true" if raw_value is None else raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized not in {"0", "false", "no", "off"}:
        raise RuntimeConfigurationError("VERIFY_SSL must be a strict boolean")
    if app_env.strip().lower() != "test" or not allow_insecure_test:
        raise RuntimeConfigurationError(
            "VERIFY_SSL=false is allowed only with explicit test break-glass"
        )
    return False


def validate_database_credentials(*, db_dsn, db_password):
    if not (db_dsn or "").strip() and not (db_password or "").strip():
        raise RuntimeConfigurationError("Database credentials must be explicitly configured")


_LEGACY_SEAMS = (
    "API_SESSION", "BASE_URL", "API_TOKEN", "ACCOUNT_STATUS", "PORTFOLIO_CURRENCY",
    "HTTP_TIMEOUT_SECONDS", "HTTP_RETRY_TOTAL", "HTTP_BACKOFF_SECONDS",
    "HTTP_MAX_BACKOFF_SECONDS", "INSTRUMENT_CACHE_TTL_SECONDS",
    "INSTRUMENT_CACHE_MAX_ENTRIES", "OPERATIONS_MAX_PAGES", "VERIFY_SSL", "logger",
    "post_api", "_iter_operation_pages", "api_get_portfolio",
    "api_get_instrument_by_figi", "api_get_bond_coupons", "api_get_dividends",
    "get_latest_cost_basis", "local_today", "dt_to_iso_z", "SessionLocal",
    "TINKOFF_ACCOUNT_ID",
)


def _sync_legacy_seams() -> None:
    """Preserve monkeypatch-friendly tracker.app compatibility during extraction."""
    for name in _LEGACY_SEAMS:
        value = globals()[name]
        setattr(_legacy, name, _LEGACY_ORIGINALS.get(name) if value is _FACADE_EXPORTS.get(name) else value)


_legacy_post_api = _legacy.post_api

def post_api(method_path: str, payload: dict) -> dict:
    _sync_legacy_seams()
    return _legacy_post_api(method_path, payload)


def _delegate(name):
    def delegated(*args, **kwargs):
        _sync_legacy_seams()
        return getattr(_legacy, name)(*args, **kwargs)

    delegated.__name__ = name
    return delegated

api_get_accounts = _delegate("api_get_accounts")
api_get_portfolio = _delegate("api_get_portfolio")
api_get_instrument_by_figi = _delegate("api_get_instrument_by_figi")
api_get_bond_coupons = _delegate("api_get_bond_coupons")
api_get_dividends = _delegate("api_get_dividends")
_iter_operation_pages = _delegate("_iter_operation_pages")
sync_payout_calendar_for_account = _delegate("sync_payout_calendar_for_account")
_sync_operations = _delegate("_sync_operations")
_reconcile_income_events = _delegate("_reconcile_income_events")
sync_operations = _delegate("sync_operations")
sync_operations_for_account = _delegate("sync_operations_for_account")
take_snapshot_for_account = _delegate("take_snapshot_for_account")


def choose_account(accounts_data: dict) -> dict:
    _sync_legacy_seams()
    return _legacy.choose_account(accounts_data)

_FACADE_EXPORTS = {
    name: globals()[name]
    for name in (
        "post_api", "api_get_accounts", "api_get_portfolio", "api_get_instrument_by_figi",
        "api_get_bond_coupons", "api_get_dividends", "_iter_operation_pages",
    )
}
_LEGACY_ORIGINALS = {name: getattr(_legacy, name) for name in _FACADE_EXPORTS}

def api_get_operations_by_cursor(account_id, opened_iso):
    for operations in _iter_operation_pages(account_id, opened_iso):
        yield from operations


def job_with_retry() -> bool:
    try:
        logger.info("snapshot_job_started", "Snapshot job started.")
        if not run_snapshot_and_operations_once():
            logger.error(
                "snapshot_job_incomplete",
                "Snapshot job did not complete all required synchronization steps.",
            )
            return False
        write_tracker_ready_state()
        logger.info("snapshot_job_completed", "Snapshot job completed successfully.")
        return True
    except Exception:
        logger.exception("snapshot_job_failed", "Snapshot job failed.")
        return False


def main() -> int:
    clear_tracker_ready_state()
    try:
        validate_database_credentials(db_dsn=EXPLICIT_DB_DSN, db_password=DB_PASSWORD)
    except RuntimeConfigurationError as exc:
        logger.error(
            "invalid_runtime_configuration",
            "Required tracker runtime configuration is missing or malformed.",
            {"error_type": type(exc).__name__},
        )
        return 1

    if not API_TOKEN and os.getenv("TRACKER_STARTUP_MODE", "").strip() != "container-smoke":
        logger.error("missing_api_token", "TINVEST_API_TOKEN не задан. Передай его через переменную окружения.")
        return 1

    init_db()
    try:
        if run_container_smoke_if_requested(
            mode=os.getenv("TRACKER_STARTUP_MODE", ""), app_env=APP_ENV,
            write_ready_state=write_tracker_ready_state,
        ):
            return 0
    except ContainerSmokeConfigurationError:
        logger.error("tracker_container_smoke_invalid_configuration", "Tracker CI smoke mode requires APP_ENV=ci.")
        return 1

    if not job_with_retry():
        return 1
    payout_calendar_job_with_retry()

    scheduler = BlockingScheduler(timezone=SCHED_TZ)
    scheduler.add_job(
        payout_calendar_job_with_retry,
        CronTrigger(hour=PAYOUT_CALENDAR_SYNC_HOUR, minute=PAYOUT_CALENDAR_SYNC_MINUTE, timezone=LOCAL_TZ),
        name="daily_payout_calendar_sync", misfire_grace_time=3600, replace_existing=True,
    )
    logger.info(
        "payout_calendar_schedule_registered", "Payout calendar schedule registered.",
        {"hour": PAYOUT_CALENDAR_SYNC_HOUR, "minute": PAYOUT_CALENDAR_SYNC_MINUTE,
         "timezone": SCHED_TZ, "horizon_days": PAYOUT_CALENDAR_HORIZON_DAYS},
    )

    if SNAPSHOT_MODE == "cron":
        trigger = CronTrigger(hour=SNAPSHOT_HOUR, minute=SNAPSHOT_MINUTE)
        name, grace, context = "daily_snapshot", 3600, {
            "mode": "cron", "snapshot_hour": SNAPSHOT_HOUR,
            "snapshot_minute": SNAPSHOT_MINUTE, "timezone": SCHED_TZ,
        }
    else:
        trigger = IntervalTrigger(minutes=SNAPSHOT_INTERVAL_MINUTES)
        name, grace, context = "interval_snapshot", SNAPSHOT_INTERVAL_MINUTES * 60, {
            "mode": "interval", "snapshot_interval_minutes": SNAPSHOT_INTERVAL_MINUTES,
            "timezone": SCHED_TZ,
        }
    scheduler.add_job(job_with_retry, trigger, name=name, misfire_grace_time=grace, replace_existing=True)
    if SNAPSHOT_MODE == "cron":
        logger.info("scheduler_started", "Scheduler started.", context)
    else:
        logger.info("interval_snapshot", "Scheduler started.", context)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("service_stopped", "Service stopped.")
    clear_tracker_ready_state()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception("tracker_process_failed", "Tracker process terminated with an unhandled exception.")
        raise SystemExit(1)
