"""
iis_tracker: ежедневные снапшоты портфеля + синхронизация операций
для одного T-Invest счёта (ИИС).

Функционал:
- при старте: один раз делаем снапшот за текущий день (перезапись дня);
- дальше: по расписанию (каждые SNAPSHOT_INTERVAL_MINUTES минут) делаем снапшот;
- для каждого дня:
    * сохраняем агрегаты по портфелю;
    * сохраняем состав портфеля (позиции);
    * синхронизируем операции в таблицу operations.
- при старте и раз в сутки синхронизируем календарь будущих купонов
  и объявленных дивидендов.
"""

import os
from datetime import datetime, timezone, date
from typing import Optional
from zoneinfo import ZoneInfo
import urllib3


# Import unified JSON logging setup
from financetracker.common.logging_setup import configure_logging, get_logger
from financetracker.common.readiness_state import clear_ready_state, write_ready_state
from financetracker.common.time_utils import utc_now
from financetracker.common.runtime_paths import configured_runtime_file
from financetracker.config.database import read_database_settings
from financetracker.config.tracker import read_tracker_http_settings
from financetracker.database.base import Base
from financetracker.database import models as database_models
from financetracker.database.session import create_session_factory
from financetracker.tracker.http_session import build_http_session
from financetracker.tracker.payout_calendar_sync import (
    sync_payout_calendar_for_account as _sync_payout_calendar_for_account,
)
from financetracker.tracker import operations_sync as _operations_sync
from financetracker.tracker import snapshot_sync as _snapshot_sync
from financetracker.tracker import tinvest_client as _tinvest_client

# Configure logging once at import
configure_logging()

logger = get_logger(__name__)


def __getattr__(name):
    if name in {
        "IncomeEvent", "Instrument", "Operation", "PayoutCalendarEvent",
        "PortfolioPosition", "PortfolioSnapshot",
    }:
        return getattr(database_models, name)
    raise AttributeError(name)


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

# ============ CONFIG из окружения ============

_HTTP_SETTINGS = read_tracker_http_settings()
API_TOKEN = _HTTP_SETTINGS.api_token
BASE_URL = _HTTP_SETTINGS.base_url
ACCOUNT_STATUS = _HTTP_SETTINGS.account_status
PORTFOLIO_CURRENCY = _HTTP_SETTINGS.portfolio_currency
HTTP_TIMEOUT_SECONDS = _HTTP_SETTINGS.timeout_seconds
HTTP_RETRY_TOTAL = _HTTP_SETTINGS.retry_total
HTTP_BACKOFF_SECONDS = _HTTP_SETTINGS.backoff_seconds
HTTP_MAX_BACKOFF_SECONDS = _HTTP_SETTINGS.max_backoff_seconds
HTTP_POOL_CONNECTIONS = _HTTP_SETTINGS.pool_connections
HTTP_POOL_MAXSIZE = _HTTP_SETTINGS.pool_maxsize
INSTRUMENT_CACHE_TTL_SECONDS = _HTTP_SETTINGS.instrument_cache_ttl_seconds
INSTRUMENT_CACHE_MAX_ENTRIES = _HTTP_SETTINGS.instrument_cache_max_entries
OPERATIONS_MAX_PAGES = max(1, int(os.getenv("OPERATIONS_MAX_PAGES", "10000")))
PAYOUT_CALENDAR_HORIZON_DAYS = max(
    1,
    int(os.getenv("PAYOUT_CALENDAR_HORIZON_DAYS", "90")),
)
PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS = max(
    1,
    int(os.getenv("PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS", "365")),
)
PAYOUT_CALENDAR_SYNC_HOUR = int(os.getenv("PAYOUT_CALENDAR_SYNC_HOUR", "9"))
PAYOUT_CALENDAR_SYNC_MINUTE = int(os.getenv("PAYOUT_CALENDAR_SYNC_MINUTE", "0"))

# Можно зафиксировать конкретный account_id, если надо
TINKOFF_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID", "")

# Время снапшота (по таймзоне SCHED_TZ).
SNAPSHOT_HOUR = int(os.getenv("SNAPSHOT_HOUR", "23"))   # раньше было 23:30 по Москве
SNAPSHOT_MINUTE = int(os.getenv("SNAPSHOT_MINUTE", "30"))
SCHED_TZ = os.getenv("SCHED_TZ", "Europe/Moscow")
TIMEZONE_NAME = os.getenv("TIMEZONE", SCHED_TZ).strip() or SCHED_TZ
try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except Exception:
    # Если в образе нет tzdata, ZoneInfo может не найти базу таймзон.
    # В таком случае не падаем, а работаем в UTC.
    LOCAL_TZ = ZoneInfo("UTC")


# Интервал обновления снапшота в минутах (по умолчанию: каждые 5 минут)
SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))

# interval | cron
SNAPSHOT_MODE = os.getenv("SNAPSHOT_MODE", "interval").strip().lower()
TRACKER_READY_FILE = configured_runtime_file("TRACKER_READY_FILE", "financetracker-tracker.ready")

APP_ENV = os.getenv("APP_ENV", "dev")
ALLOW_INSECURE_TLS_FOR_TESTS = (
    os.getenv("ALLOW_INSECURE_TLS_FOR_TESTS", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
VERIFY_SSL_ENV = os.getenv("VERIFY_SSL")
VERIFY_SSL = parse_verify_ssl(
    VERIFY_SSL_ENV,
    app_env=APP_ENV,
    allow_insecure_test=ALLOW_INSECURE_TLS_FOR_TESTS,
)

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Сообщаем в лог, что SSL-проверка отключена
    logger.warning(
        "ssl_verification_disabled",
        "VERIFY_SSL=false — SSL-проверка отключена. Используй только в доверенной сети.",
    )

# Настройки Postgres
_DATABASE_SETTINGS = read_database_settings()
DB_PASSWORD = _DATABASE_SETTINGS.password
EXPLICIT_DB_DSN = _DATABASE_SETTINGS.explicit_dsn
DB_DSN = _DATABASE_SETTINGS.dsn

# ============ INIT DB ============

engine, SessionLocal = create_session_factory(DB_DSN)
API_SESSION = build_http_session(
    pool_connections=HTTP_POOL_CONNECTIONS,
    pool_maxsize=HTTP_POOL_MAXSIZE,
)


def init_db():
    Base.metadata.create_all(bind=engine)


def _configure_tinvest_client() -> None:
    _tinvest_client.configure(API_SESSION=API_SESSION, BASE_URL=BASE_URL, API_TOKEN=API_TOKEN, ACCOUNT_STATUS=ACCOUNT_STATUS, PORTFOLIO_CURRENCY=PORTFOLIO_CURRENCY, HTTP_TIMEOUT_SECONDS=HTTP_TIMEOUT_SECONDS, HTTP_RETRY_TOTAL=HTTP_RETRY_TOTAL, HTTP_BACKOFF_SECONDS=HTTP_BACKOFF_SECONDS, HTTP_MAX_BACKOFF_SECONDS=HTTP_MAX_BACKOFF_SECONDS, INSTRUMENT_CACHE_TTL_SECONDS=INSTRUMENT_CACHE_TTL_SECONDS, INSTRUMENT_CACHE_MAX_ENTRIES=INSTRUMENT_CACHE_MAX_ENTRIES, OPERATIONS_MAX_PAGES=OPERATIONS_MAX_PAGES, VERIFY_SSL=VERIFY_SSL, logger=logger)


def post_api(method_path: str, payload: dict) -> dict:
    _configure_tinvest_client()
    return _tinvest_client._post_api_impl(method_path, payload)


def _client_call(callable_):
    _configure_tinvest_client()
    _tinvest_client.post_api = post_api
    return callable_()


def api_get_accounts() -> dict:
    return _client_call(_tinvest_client.api_get_accounts)


def api_get_portfolio(account_id: str) -> dict:
    return _client_call(lambda: _tinvest_client.api_get_portfolio(account_id))


def api_get_instrument_by_figi(figi: str):
    return _client_call(lambda: _tinvest_client.api_get_instrument_by_figi(figi))


def api_get_bond_coupons(instrument_id: str, from_iso: str, to_iso: str):
    return _client_call(lambda: _tinvest_client.api_get_bond_coupons(instrument_id, from_iso, to_iso))


def api_get_dividends(instrument_id: str, from_iso: str, to_iso: str):
    return _client_call(lambda: _tinvest_client.api_get_dividends(instrument_id, from_iso, to_iso))


OperationsPaginationError = _tinvest_client.OperationsPaginationError
_INSTRUMENT_CACHE = _tinvest_client._INSTRUMENT_CACHE


def _iter_operation_pages(account_id: str, from_date: Optional[str], **kwargs):
    return _client_call(lambda: _tinvest_client._iter_operation_pages(account_id, from_date, **kwargs))


def api_get_operations_by_cursor(account_id: str, opened_iso: Optional[str]):
    for operations in _iter_operation_pages(account_id, opened_iso):
        yield from operations


# ============ HELPERS ============



# ============ ЛОГИКА СЕРВИСА ============

def parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def local_today() -> date:
    """Current local date in SCHED_TZ."""
    return datetime.now(LOCAL_TZ).date()


def dt_to_iso_z(dt_val: datetime) -> str:
    """Convert datetime to ISO8601 string with 'Z' (UTC)."""
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    else:
        dt_val = dt_val.astimezone(timezone.utc)
    return dt_val.strftime("%Y-%m-%dT%H:%M:%SZ")


def sync_payout_calendar_for_account(db, account_id: str) -> dict[str, int]:
    """Compatibility facade that supplies tracker runtime dependencies."""
    return _sync_payout_calendar_for_account(
        db,
        account_id,
        api_get_bond_coupons=api_get_bond_coupons,
        api_get_dividends=api_get_dividends,
        local_today=local_today,
        local_tz=LOCAL_TZ,
        horizon_days=PAYOUT_CALENDAR_HORIZON_DAYS,
        dividend_record_lookback_days=PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS,
        logger=logger,
    )


guess_deposit_source = _operations_sync.guess_deposit_source
_upsert_operation = _operations_sync._upsert_operation
get_latest_cost_basis = _operations_sync.get_latest_cost_basis


def _sync_operations(db, account_id: str, from_date: Optional[str], *, affected_income_keys=None):
    return _operations_sync._sync_operations(
        db,
        account_id,
        from_date,
        operation_pages=_iter_operation_pages,
        local_tz=LOCAL_TZ,
        logger=logger,
        affected_income_keys=affected_income_keys,
    )


def _reconcile_income_events(db, account_id: str, affected_keys):
    return _operations_sync._reconcile_income_events(
        db,
        account_id,
        affected_keys,
        local_tz=LOCAL_TZ,
        get_cost_basis=get_latest_cost_basis,
    )


def sync_operations(account_id: str, from_date: Optional[str]) -> dict:
    with SessionLocal() as db:
        stats = _sync_operations(db, account_id, from_date)
        db.commit()
        return stats


def sync_operations_for_account(db, acc_data: dict):
    return _operations_sync.sync_operations_for_account(
        db,
        acc_data,
        operation_pages=_iter_operation_pages,
        dt_to_iso_z=dt_to_iso_z,
        local_tz=LOCAL_TZ,
        logger=logger,
    )


ensure_instrument = _snapshot_sync.ensure_instrument
compute_expected_yield_pct = _snapshot_sync.compute_expected_yield_pct


def take_snapshot_for_account(db, acc_data: dict):
    return _snapshot_sync.take_snapshot_for_account(
        db,
        acc_data,
        api_get_portfolio=api_get_portfolio,
        api_get_instrument_by_figi=api_get_instrument_by_figi,
        utc_now=utc_now,
        local_today=local_today,
        portfolio_currency=PORTFOLIO_CURRENCY,
        logger=logger,
    )



def choose_account(accounts_data: dict) -> dict:
    """
    Выбираем один счёт:
    - если TINKOFF_ACCOUNT_ID задан — по нему;
    - иначе: первый открытый, если есть; иначе просто первый.
    """
    accounts = accounts_data.get("accounts") or []
    if not accounts:
        raise RuntimeError("No accounts returned from API")

    configured_account_id = (TINKOFF_ACCOUNT_ID or "").strip()
    if configured_account_id and configured_account_id.lower() != "auto":
        for acc in accounts:
            if str(acc.get("id")) == configured_account_id:
                return acc
        raise RuntimeError("Configured TINKOFF_ACCOUNT_ID was not found")

    open_accounts = [a for a in accounts if a.get("status") == "ACCOUNT_STATUS_OPEN"]
    if len(open_accounts) == 1:
        return open_accounts[0]
    raise RuntimeError("Automatic account selection requires exactly one open account")



def clear_tracker_ready_state() -> None:
    clear_ready_state(TRACKER_READY_FILE)


def write_tracker_ready_state() -> None:
    write_ready_state(TRACKER_READY_FILE)


def run_snapshot_and_operations_once() -> bool:
    accounts_data = api_get_accounts()
    acc = choose_account(accounts_data)

    with SessionLocal() as db:
        # 1) Снапшот не должен зависеть от синка пополнений.
        take_snapshot_for_account(db, acc)
        db.commit()

        # 2) Операции — вторым шагом (если упадёт, снапшот всё равно останется актуальным).
        try:
            sync_operations_for_account(db, acc)
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "operations_sync_failed",
                "Operations sync failed; snapshot remains saved.",
            )
            return False
    return True


def run_payout_calendar_sync_once():
    accounts_data = api_get_accounts()
    account = choose_account(accounts_data)
    account_id = str(account.get("id"))

    with SessionLocal() as db:
        stats = sync_payout_calendar_for_account(db, account_id)
        db.commit()

    logger.info(
        "payout_calendar_sync_completed",
        "Payout calendar sync completed.",
        {"account_id": account_id, **stats},
    )


def job_with_retry() -> bool:
    """
    Обёртка для планировщика:
    - одна попытка на запуск;
    - без sleep() внутри;
    - при ошибке всё сделает следующий запуск по расписанию.
    """
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


def payout_calendar_job_with_retry():
    try:
        logger.info(
            "payout_calendar_sync_started",
            "Payout calendar sync started.",
            {"horizon_days": PAYOUT_CALENDAR_HORIZON_DAYS},
        )
        run_payout_calendar_sync_once()
    except Exception:
        logger.exception(
            "payout_calendar_sync_failed",
            "Payout calendar sync failed; previously cached rows are preserved.",
        )
