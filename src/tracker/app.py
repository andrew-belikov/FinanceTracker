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
import json
import copy
import hashlib
import textwrap
import time
import traceback
from collections import OrderedDict
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Optional
from zoneinfo import ZoneInfo
import requests
import urllib3
from requests.adapters import HTTPAdapter
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    BigInteger,
    String,
    Text,
    Date,
    DateTime,
    Numeric,
    Boolean,
    ForeignKey,
    Index,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Import unified JSON logging setup
from common.logging_setup import configure_logging, get_logger
from income_events import compute_income_net_amount, compute_income_net_yield_pct

# Configure logging once at import
configure_logging()

logger = get_logger(__name__)

MAX_LOG_RESPONSE_BODY_CHARS = 4000

# ============ CONFIG из окружения ============

# Токен T-Invest. Лучше передавать через ENV, но можно и захардкодить здесь.
API_TOKEN = os.getenv("TINVEST_API_TOKEN", "").strip()

BASE_URL = os.getenv(
    "TINVEST_BASE_URL",
    "https://invest-public-api.tbank.ru/rest",
)

ACCOUNT_STATUS = os.getenv("TINVEST_ACCOUNT_STATUS", "ACCOUNT_STATUS_ALL")
PORTFOLIO_CURRENCY = os.getenv("TINVEST_PORTFOLIO_CURRENCY", "RUB")
HTTP_TIMEOUT_SECONDS = float(os.getenv("TINVEST_HTTP_TIMEOUT_SECONDS", "20"))
HTTP_RETRY_TOTAL = max(0, int(os.getenv("TINVEST_HTTP_RETRY_TOTAL", "3")))
HTTP_BACKOFF_SECONDS = max(0.0, float(os.getenv("TINVEST_HTTP_BACKOFF_SECONDS", "1")))
HTTP_MAX_BACKOFF_SECONDS = max(
    0.0,
    float(os.getenv("TINVEST_HTTP_MAX_BACKOFF_SECONDS", "60")),
)
HTTP_POOL_CONNECTIONS = max(1, int(os.getenv("TINVEST_HTTP_POOL_CONNECTIONS", "8")))
HTTP_POOL_MAXSIZE = max(1, int(os.getenv("TINVEST_HTTP_POOL_MAXSIZE", "8")))
INSTRUMENT_CACHE_TTL_SECONDS = max(
    0.0,
    float(os.getenv("TINVEST_INSTRUMENT_CACHE_TTL_SECONDS", "86400")),
)
INSTRUMENT_CACHE_MAX_ENTRIES = max(
    0,
    int(os.getenv("TINVEST_INSTRUMENT_CACHE_MAX_ENTRIES", "1024")),
)
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
try:
    LOCAL_TZ = ZoneInfo(SCHED_TZ)
except Exception:
    # Если в образе нет tzdata, ZoneInfo может не найти базу таймзон.
    # В таком случае не падаем, а работаем в UTC.
    LOCAL_TZ = ZoneInfo("UTC")


# Интервал обновления снапшота в минутах (по умолчанию: каждые 5 минут)
SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))

# interval | cron
SNAPSHOT_MODE = os.getenv("SNAPSHOT_MODE", "interval").strip().lower()

# SSL-проверка (у тебя сейчас нужен режим БЕЗ проверки)
VERIFY_SSL_ENV = os.getenv("VERIFY_SSL", "false").lower()
VERIFY_SSL = VERIFY_SSL_ENV in ("1", "true", "yes")

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Сообщаем в лог, что SSL-проверка отключена
    logger.warning(
        "ssl_verification_disabled",
        "VERIFY_SSL=false — SSL-проверка отключена. Используй только в доверенной сети.",
    )

# Настройки Postgres
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "fintracker")
DB_USER = os.getenv("DB_USER", "aqua4")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Q1a2z334")

DB_DSN = os.getenv(
    "DB_DSN",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

Base = declarative_base()

# ============ МОДЕЛИ БД ============

class Instrument(Base):
    __tablename__ = "instruments"

    id = Column(Integer, primary_key=True)
    figi = Column(String, unique=True, nullable=False)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    class_code = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "snapshot_date",
            name="uq_snapshot_account_date",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)  # ID счёта в Т-Инвест
    account_name = Column(String, nullable=True)

    snapshot_at = Column(DateTime, nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)

    currency = Column(String, nullable=False)

    total_value = Column(Numeric(18, 2), nullable=True)
    total_shares = Column(Numeric(18, 2), nullable=True)
    total_bonds = Column(Numeric(18, 2), nullable=True)
    total_etf = Column(Numeric(18, 2), nullable=True)
    total_currencies = Column(Numeric(18, 2), nullable=True)
    total_futures = Column(Numeric(18, 2), nullable=True)
    expected_yield = Column(Numeric(18, 2), nullable=True)
    expected_yield_pct = Column(Numeric(9, 4), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    positions = relationship(
        "PortfolioPosition",
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )


class PortfolioPosition(Base):
    __tablename__ = "portfolio_positions"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("portfolio_snapshots.id"), nullable=False)

    figi = Column(String, nullable=False)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=True)
    instrument_uid = Column(String, nullable=True)
    position_uid = Column(String, nullable=True)
    asset_uid = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)

    quantity = Column(Numeric(18, 6), nullable=True)
    currency = Column(String, nullable=True)

    current_price = Column(Numeric(18, 4), nullable=True)
    current_nkd = Column(Numeric(18, 9), nullable=True)
    position_value = Column(Numeric(18, 2), nullable=True)
    expected_yield = Column(Numeric(18, 2), nullable=True)
    expected_yield_pct = Column(Numeric(9, 4), nullable=True)
    weight_pct = Column(Numeric(9, 4), nullable=True)
    raw_payload_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    snapshot = relationship("PortfolioSnapshot", back_populates="positions")
    instrument = relationship("Instrument")


class Operation(Base):
    __tablename__ = "operations"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "operation_id",
            name="uq_operations_account_operation",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)

    operation_id = Column(String, nullable=False)
    operation_type = Column(String, nullable=False)
    cashflow_category = Column(String, nullable=True)
    cursor = Column(String, nullable=True)
    broker_account_id = Column(String, nullable=True)
    parent_operation_id = Column(String, nullable=True)
    name = Column(String, nullable=True)
    state = Column(String, nullable=True)
    instrument_uid = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    instrument_kind = Column(String, nullable=True)
    position_uid = Column(String, nullable=True)
    asset_uid = Column(String, nullable=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    price = Column(Numeric(18, 9), nullable=True)
    commission = Column(Numeric(18, 9), nullable=True)
    yield_amount = Column("yield", Numeric(18, 9), nullable=True)
    yield_relative = Column(Numeric(18, 9), nullable=True)
    accrued_int = Column(Numeric(18, 9), nullable=True)
    quantity = Column(BigInteger, nullable=True)
    quantity_rest = Column(BigInteger, nullable=True)
    quantity_done = Column(BigInteger, nullable=True)
    currency = Column(String, nullable=False)
    cancel_date_time = Column(DateTime, nullable=True)
    cancel_reason = Column(String, nullable=True)

    description = Column(String, nullable=True)
    source = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class IncomeEvent(Base):
    __tablename__ = "income_events"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "figi",
            "event_date",
            "event_type",
            name="uq_income_events_account_figi_date_type",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    figi = Column(String, nullable=False)
    event_date = Column(Date, nullable=False)
    event_type = Column(String, nullable=False)
    gross_amount = Column(Numeric(18, 2), nullable=False)
    tax_amount = Column(Numeric(18, 2), nullable=False)
    net_amount = Column(Numeric(18, 2), nullable=False)
    net_yield_pct = Column(Numeric(9, 4), nullable=False)
    notified = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class PayoutCalendarEvent(Base):
    __tablename__ = "payout_calendar_events"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "figi",
            "event_type",
            "event_uid",
            name="uq_payout_calendar_event_source",
        ),
        Index(
            "ix_payout_calendar_events_account_payment",
            "account_id",
            "payment_date",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    figi = Column(String, nullable=False)
    instrument_uid = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    event_type = Column(String, nullable=False)
    event_uid = Column(String, nullable=False)
    payment_date = Column(Date, nullable=False, index=True)
    record_date = Column(Date, nullable=True)
    last_buy_date = Column(Date, nullable=True)
    coupon_start_date = Column(Date, nullable=True)
    coupon_end_date = Column(Date, nullable=True)
    coupon_period_days = Column(Integer, nullable=True)
    amount_per_unit = Column(Numeric(18, 9), nullable=True)
    quantity = Column(Numeric(18, 6), nullable=False)
    expected_amount = Column(Numeric(18, 2), nullable=True)
    currency = Column(String, nullable=True)
    source_event_type = Column(String, nullable=True)
    fetched_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class AssetAlias(Base):
    __tablename__ = "asset_aliases"
    __table_args__ = (
        UniqueConstraint(
            "asset_uid",
            "instrument_uid",
            "figi",
            name="uq_asset_aliases_asset_instrument_figi",
        ),
    )

    id = Column(Integer, primary_key=True)
    asset_uid = Column(String, nullable=False)
    instrument_uid = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    first_seen_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class RebalanceTarget(Base):
    __tablename__ = "rebalance_targets"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "asset_class",
            name="uq_rebalance_targets_account_class",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    asset_class = Column(String, nullable=False)
    target_weight_pct = Column(Numeric(9, 4), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, server_default=func.now(), nullable=False)


class InvestNotification(Base):
    __tablename__ = "invest_notifications"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "operation_id",
            name="uq_invest_notifications_account_operation",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    operation_id = Column(String, nullable=False)
    operation_date = Column(DateTime, nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, server_default=func.now(), nullable=False)


# ============ INIT DB ============

engine = create_engine(DB_DSN, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def init_db():
    Base.metadata.create_all(bind=engine)


# ============ HELPERS ============

def _to_int(v, default=0):
    if v is None:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def quotation_to_float(q: Optional[dict]) -> Optional[float]:
    if not q:
        return None
    units = _to_int(q.get("units"))
    nano = _to_int(q.get("nano"))
    return units + nano / 1e9


def money_to_float(m: Optional[dict]) -> Optional[float]:
    if not m:
        return None
    units = _to_int(m.get("units"))
    nano = _to_int(m.get("nano"))
    return units + nano / 1e9


def get_json_value(payload: dict, snake_name: str):
    camel_name = "".join(
        part.capitalize() if i else part
        for i, part in enumerate(snake_name.split("_"))
    )
    return payload.get(camel_name, payload.get(snake_name))


def upsert_asset_alias(
    db,
    *,
    asset_uid: Optional[str],
    instrument_uid: Optional[str],
    figi: Optional[str],
    name: Optional[str],
    seen_at: Optional[datetime],
):
    if not asset_uid:
        return

    seen_at = seen_at or datetime.now(timezone.utc).replace(tzinfo=None)
    instrument = None
    if figi:
        instrument = db.query(Instrument).filter(Instrument.figi == figi).one_or_none()

    ticker = instrument.ticker if instrument is not None else None
    display_name = name or (instrument.name if instrument is not None else None) or figi or asset_uid

    alias = (
        db.query(AssetAlias)
        .filter(
            AssetAlias.asset_uid == asset_uid,
            AssetAlias.instrument_uid == instrument_uid,
            AssetAlias.figi == figi,
        )
        .one_or_none()
    )
    if alias is None:
        alias = AssetAlias(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
            ticker=ticker,
            name=display_name,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
        )
        db.add(alias)
        return

    alias.ticker = ticker or alias.ticker
    alias.name = display_name or alias.name
    if seen_at < alias.first_seen_at:
        alias.first_seen_at = seen_at
    if seen_at > alias.last_seen_at:
        alias.last_seen_at = seen_at
    alias.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)


def resolve_asset_uid_for_position(
    db,
    *,
    asset_uid: Optional[str],
    instrument_uid: Optional[str],
    figi: Optional[str],
) -> Optional[str]:
    if asset_uid:
        return asset_uid

    alias = None
    if instrument_uid:
        alias = (
            db.query(AssetAlias)
            .filter(AssetAlias.instrument_uid == instrument_uid)
            .order_by(AssetAlias.last_seen_at.desc(), AssetAlias.id.desc())
            .first()
        )
    if alias is None and figi:
        alias = (
            db.query(AssetAlias)
            .filter(AssetAlias.figi == figi)
            .order_by(AssetAlias.last_seen_at.desc(), AssetAlias.id.desc())
            .first()
        )
    return alias.asset_uid if alias is not None else None


def _url_host(url: str) -> str:
    return url.split("//", 1)[1].split("/", 1)[0]


def _url_path(url: str) -> str:
    parts = url.split("/", 3)
    return "/" + parts[3] if len(parts) > 3 else "/"


def _truncate_log_text(value: str, limit: int = MAX_LOG_RESPONSE_BODY_CHARS) -> tuple[str, bool]:
    if len(value) <= limit:
        return value, False
    return value[:limit] + "...<truncated>", True


def _build_response_body_ctx(resp, base_ctx: Optional[dict] = None) -> dict:
    ctx = dict(base_ctx or {})
    content_type = resp.headers.get("Content-Type")
    if content_type:
        ctx["content_type"] = content_type

    try:
        ctx["response_body"] = resp.json()
        ctx["response_body_truncated"] = False
        return ctx
    except Exception:
        truncated_body, truncated = _truncate_log_text(resp.text)
        ctx["response_body"] = truncated_body
        ctx["response_body_truncated"] = truncated
        return ctx


def _build_api_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=HTTP_POOL_CONNECTIONS,
        pool_maxsize=HTTP_POOL_MAXSIZE,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


API_SESSION = _build_api_session()
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, *range(500, 600)})
RETRYABLE_REQUEST_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def _parse_retry_delay(value: Optional[str], *, now: Optional[datetime] = None) -> Optional[float]:
    if value is None:
        return None

    raw_value = value.strip()
    if not raw_value:
        return None

    try:
        delay = float(raw_value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current_time = now or datetime.now(timezone.utc)
        delay = (retry_at - current_time).total_seconds()

    if delay < 0:
        return None
    return min(delay, HTTP_MAX_BACKOFF_SECONDS)


def _retry_delay_seconds(resp, attempt: int) -> float:
    delay = _parse_retry_delay(resp.headers.get("Retry-After"))
    if delay is None and resp.status_code == 429:
        delay = _parse_retry_delay(resp.headers.get("x-ratelimit-reset"))
    if delay is None:
        delay = HTTP_BACKOFF_SECONDS * (2 ** (attempt - 1))
    return min(max(0.0, delay), HTTP_MAX_BACKOFF_SECONDS)


def _retry_log_ctx(
    method_path: str,
    *,
    attempt: int,
    delay_seconds: float,
    resp=None,
    exception_type: Optional[str] = None,
) -> dict:
    ctx = {
        "method_path": method_path,
        "attempt": attempt,
        "max_attempts": HTTP_RETRY_TOTAL + 1,
        "delay_seconds": delay_seconds,
    }
    if exception_type:
        ctx["exception_type"] = exception_type
    if resp is not None:
        ctx["status_code"] = resp.status_code
        for header_name in (
            "x-tracking-id",
            "x-ratelimit-limit",
            "x-ratelimit-remaining",
            "x-ratelimit-reset",
        ):
            value = resp.headers.get(header_name)
            if value is not None:
                ctx[header_name.replace("-", "_")] = value
    return ctx


def post_api(method_path: str, payload: dict) -> dict:
    url = f"{BASE_URL}/{method_path}"

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    # Все текущие wrapper-ы вызывают read-only Get* методы. Если здесь появится
    # мутационный RPC (например, выставление заявки), автоматические повторы
    # должны быть отключены для него отдельно.
    max_attempts = HTTP_RETRY_TOTAL + 1
    for attempt in range(1, max_attempts + 1):
        try:
            resp = API_SESSION.post(
                url,
                headers=headers,
                json=payload,
                timeout=HTTP_TIMEOUT_SECONDS,
                verify=VERIFY_SSL,
            )
        except requests.exceptions.SSLError:
            logger.exception(
                "api_request_ssl_error",
                "SSL error while calling T-Invest API.",
                {"method_path": method_path, "url_host": _url_host(url)},
            )
            raise
        except RETRYABLE_REQUEST_EXCEPTIONS as exc:
            if attempt >= max_attempts:
                logger.exception(
                    "api_request_failed",
                    "HTTP request to T-Invest API failed after retries.",
                    {
                        "method_path": method_path,
                        "url_host": _url_host(url),
                        "attempts": attempt,
                    },
                )
                raise

            delay_seconds = min(
                HTTP_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                HTTP_MAX_BACKOFF_SECONDS,
            )
            logger.warning(
                "api_retry_scheduled",
                "Retrying a failed T-Invest API request.",
                _retry_log_ctx(
                    method_path,
                    attempt=attempt,
                    delay_seconds=delay_seconds,
                    exception_type=type(exc).__name__,
                ),
            )
            time.sleep(delay_seconds)
            continue
        except requests.exceptions.RequestException:
            logger.exception(
                "api_request_failed",
                "HTTP request to T-Invest API failed.",
                {"method_path": method_path, "url_host": _url_host(url)},
            )
            raise

        if resp.status_code == 200:
            break

        if resp.status_code in RETRYABLE_HTTP_STATUSES and attempt < max_attempts:
            delay_seconds = _retry_delay_seconds(resp, attempt)
            logger.warning(
                "api_retry_scheduled",
                "Retrying a retryable T-Invest API response.",
                _retry_log_ctx(
                    method_path,
                    attempt=attempt,
                    delay_seconds=delay_seconds,
                    resp=resp,
                ),
            )
            time.sleep(delay_seconds)
            continue
        break

    if resp.status_code != 200:
        error_ctx = {
            "method_path": method_path,
            "url_host": _url_host(url),
            "path": _url_path(url),
            "status_code": resp.status_code,
        }
        logger.error(
            "api_http_error",
            "T-Invest API returned a non-200 response.",
            error_ctx,
        )
        logger.error(
            "api_http_error_body",
            "Logged T-Invest API error response body.",
            _build_response_body_ctx(resp, error_ctx),
        )
        raise RuntimeError(f"API HTTP {resp.status_code}")

    try:
        return resp.json()
    except json.JSONDecodeError:
        error_ctx = {
            "method_path": method_path,
            "url_host": _url_host(url),
            "path": _url_path(url),
            "status_code": resp.status_code,
        }
        logger.error(
            "api_json_decode_error",
            "Failed to decode JSON from T-Invest API response.",
            error_ctx,
        )
        logger.error(
            "api_json_decode_error_body",
            "Logged non-JSON T-Invest API response body.",
            _build_response_body_ctx(resp, error_ctx),
        )
        raise


# ============ API WRAPPERS ============

def api_get_accounts() -> dict:
    return post_api(
        "tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts",
        {"status": ACCOUNT_STATUS},
    )


def api_get_portfolio(account_id: str) -> dict:
    return post_api(
        "tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio",
        {
            "accountId": account_id,
            "currency": PORTFOLIO_CURRENCY,
        },
    )


_INSTRUMENT_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_INSTRUMENT_CACHE_LOCK = Lock()


def _get_cached_instrument(figi: str) -> Optional[dict]:
    if INSTRUMENT_CACHE_TTL_SECONDS <= 0 or INSTRUMENT_CACHE_MAX_ENTRIES <= 0:
        return None

    now = time.monotonic()
    with _INSTRUMENT_CACHE_LOCK:
        cached = _INSTRUMENT_CACHE.get(figi)
        if cached is None:
            return None
        expires_at, instrument = cached
        if expires_at <= now:
            del _INSTRUMENT_CACHE[figi]
            return None
        _INSTRUMENT_CACHE.move_to_end(figi)
        return copy.deepcopy(instrument)


def _cache_instrument(figi: str, instrument: Optional[dict]) -> None:
    if (
        not instrument
        or INSTRUMENT_CACHE_TTL_SECONDS <= 0
        or INSTRUMENT_CACHE_MAX_ENTRIES <= 0
    ):
        return

    expires_at = time.monotonic() + INSTRUMENT_CACHE_TTL_SECONDS
    with _INSTRUMENT_CACHE_LOCK:
        _INSTRUMENT_CACHE[figi] = (expires_at, copy.deepcopy(instrument))
        _INSTRUMENT_CACHE.move_to_end(figi)
        while len(_INSTRUMENT_CACHE) > INSTRUMENT_CACHE_MAX_ENTRIES:
            _INSTRUMENT_CACHE.popitem(last=False)


def api_get_instrument_by_figi(figi: str) -> Optional[dict]:
    cached = _get_cached_instrument(figi)
    if cached is not None:
        return cached

    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy",
        {
            "idType": "INSTRUMENT_ID_TYPE_FIGI",
            "id": figi,
        },
    )
    instrument = data.get("instrument")
    _cache_instrument(figi, instrument)
    return instrument


def api_get_bond_coupons(
    instrument_id: str,
    from_iso: str,
    to_iso: str,
) -> list[dict]:
    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetBondCoupons",
        {
            "instrumentId": instrument_id,
            "from": from_iso,
            "to": to_iso,
        },
    )
    events = data.get("events") or []
    if not isinstance(events, list):
        raise RuntimeError("GetBondCoupons returned invalid events")
    return events


def api_get_dividends(
    instrument_id: str,
    from_iso: str,
    to_iso: str,
) -> list[dict]:
    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetDividends",
        {
            "instrumentId": instrument_id,
            "from": from_iso,
            "to": to_iso,
        },
    )
    dividends = data.get("dividends") or []
    if not isinstance(dividends, list):
        raise RuntimeError("GetDividends returned invalid dividends")
    return dividends


class OperationsPaginationError(RuntimeError):
    """Pagination contract violation that must roll back a partial operation sync."""


def _iter_operation_pages(
    account_id: str,
    from_date: Optional[str],
    *,
    to_iso: Optional[str] = None,
    max_pages: Optional[int] = None,
):
    """Yield complete operation pages while enforcing cursor and page limits."""
    from_iso = from_date or "2000-01-01T00:00:00Z"
    fixed_to_iso = to_iso or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    page_limit = OPERATIONS_MAX_PAGES if max_pages is None else max(1, max_pages)
    cursor = ""
    seen_cursors = {cursor}

    for page_number in range(1, page_limit + 1):
        payload = {
            "accountId": account_id,
            "from": from_iso,
            "to": fixed_to_iso,
            "cursor": cursor,
            "limit": 1000,
            "withoutTrades": True,
        }
        data = post_api(
            "tinkoff.public.invest.api.contract.v1.OperationsService/GetOperationsByCursor",
            payload,
        )
        operations = data.get("items")
        if operations is None:
            operations = data.get("operations", [])
        if not isinstance(operations, list):
            raise OperationsPaginationError("T-Invest API returned malformed operations items")

        has_next = data.get("hasNext", False)
        if not isinstance(has_next, bool):
            raise OperationsPaginationError("T-Invest API returned non-boolean hasNext")
        logger.info(
            "operations_page_loaded",
            "Loaded operations page from T-Invest API.",
            {
                "account_id": account_id,
                "page_number": page_number,
                "page_items_count": len(operations),
                "has_next": has_next,
            },
        )
        yield operations

        if not has_next:
            return

        next_cursor = data.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            logger.error(
                "operations_next_cursor_missing",
                "T-Invest API reported another page without a next cursor.",
                {"account_id": account_id, "page_number": page_number},
            )
            raise OperationsPaginationError("hasNext=true without nextCursor")

        if next_cursor in seen_cursors:
            logger.error(
                "operations_cursor_repeated",
                "T-Invest API repeated an operations cursor.",
                {"account_id": account_id, "page_number": page_number},
            )
            raise OperationsPaginationError("operations cursor repeated")

        if page_number >= page_limit:
            logger.error(
                "operations_max_pages_reached",
                "Operations page limit reached before pagination completed.",
                {
                    "account_id": account_id,
                    "page_number": page_number,
                    "max_pages": page_limit,
                },
            )
            raise OperationsPaginationError("operations page limit reached")

        seen_cursors.add(next_cursor)
        cursor = next_cursor


def api_get_operations_by_cursor(account_id: str, opened_iso: Optional[str]):
    """Compatibility iterator over operations backed by guarded page traversal."""
    for operations in _iter_operation_pages(account_id, opened_iso):
        yield from operations


# ============ ЛОГИКА СЕРВИСА ============

def parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def utc_now_naive() -> datetime:
    """UTC time without tzinfo (safe for DB columns without timezone)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def _money_to_decimal(value: Optional[dict]) -> Optional[Decimal]:
    if not isinstance(value, dict):
        return None
    units = Decimal(_to_int(value.get("units")))
    nano = Decimal(_to_int(value.get("nano"))) / Decimal("1000000000")
    return units + nano


def _iso_to_local_date(value: Optional[str]) -> Optional[date]:
    parsed = parse_iso_dt(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(LOCAL_TZ).date()


def _payout_event_uid(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _normalize_payout_instrument_type(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "instrument_type_bond": "bond",
        "instrument_type_share": "share",
        "instrument_type_etf": "etf",
        "stock": "share",
    }
    return aliases.get(normalized, normalized)


def _upsert_payout_calendar_event(
    db,
    *,
    account_id: str,
    position: PortfolioPosition,
    event_type: str,
    event_uid: str,
    payment_date: date,
    record_date: Optional[date],
    last_buy_date: Optional[date],
    coupon_start_date: Optional[date],
    coupon_end_date: Optional[date],
    coupon_period_days: Optional[int],
    amount_per_unit: Optional[Decimal],
    currency: Optional[str],
    source_event_type: Optional[str],
    fetched_at: datetime,
) -> None:
    quantity = Decimal(position.quantity or 0)
    expected_amount = None
    if amount_per_unit is not None:
        expected_amount = (amount_per_unit * quantity).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

    row = (
        db.query(PayoutCalendarEvent)
        .filter(
            PayoutCalendarEvent.account_id == account_id,
            PayoutCalendarEvent.figi == position.figi,
            PayoutCalendarEvent.event_type == event_type,
            PayoutCalendarEvent.event_uid == event_uid,
        )
        .one_or_none()
    )
    if row is None:
        row = PayoutCalendarEvent(
            account_id=account_id,
            figi=position.figi,
            event_type=event_type,
            event_uid=event_uid,
            created_at=fetched_at,
        )
        db.add(row)

    instrument = position.instrument
    row.instrument_uid = position.instrument_uid
    row.ticker = position.ticker or (instrument.ticker if instrument is not None else None)
    row.name = position.name or (instrument.name if instrument is not None else None)
    row.instrument_type = position.instrument_type
    row.payment_date = payment_date
    row.record_date = record_date
    row.last_buy_date = last_buy_date
    row.coupon_start_date = coupon_start_date
    row.coupon_end_date = coupon_end_date
    row.coupon_period_days = coupon_period_days
    row.amount_per_unit = amount_per_unit
    row.quantity = quantity
    row.expected_amount = expected_amount
    row.currency = (currency or "").upper() or None
    row.source_event_type = source_event_type
    row.fetched_at = fetched_at
    row.updated_at = fetched_at


def _replace_position_payout_events(
    db,
    *,
    account_id: str,
    position: PortfolioPosition,
    event_type: str,
    source_events: list[dict],
    period_start: date,
    period_end: date,
    fetched_at: datetime,
) -> int:
    seen_uids: set[str] = set()
    stored = 0

    for event in source_events:
        if event_type == "coupon":
            payment_date = _iso_to_local_date(get_json_value(event, "coupon_date"))
            record_date = _iso_to_local_date(get_json_value(event, "fix_date"))
            last_buy_date = None
            coupon_start_date = _iso_to_local_date(
                get_json_value(event, "coupon_start_date")
            )
            coupon_end_date = _iso_to_local_date(
                get_json_value(event, "coupon_end_date")
            )
            raw_coupon_period = get_json_value(event, "coupon_period")
            try:
                coupon_period_days = (
                    int(raw_coupon_period) if raw_coupon_period is not None else None
                )
            except (TypeError, ValueError):
                coupon_period_days = None
            money = get_json_value(event, "pay_one_bond")
            source_event_type = get_json_value(event, "coupon_type")
            event_uid = _payout_event_uid(
                event_type,
                get_json_value(event, "coupon_number"),
                get_json_value(event, "coupon_date"),
                get_json_value(event, "coupon_start_date"),
                get_json_value(event, "coupon_end_date"),
            )
        else:
            payment_date = _iso_to_local_date(get_json_value(event, "payment_date"))
            record_date = _iso_to_local_date(get_json_value(event, "record_date"))
            last_buy_date = _iso_to_local_date(get_json_value(event, "last_buy_date"))
            coupon_start_date = None
            coupon_end_date = None
            coupon_period_days = None
            money = get_json_value(event, "dividend_net")
            source_event_type = get_json_value(event, "dividend_type")
            if "cancel" in (source_event_type or "").lower():
                continue
            event_uid = _payout_event_uid(
                event_type,
                get_json_value(event, "record_date"),
                get_json_value(event, "payment_date"),
                get_json_value(event, "declared_date"),
                source_event_type,
            )

        if payment_date is None or not (period_start <= payment_date <= period_end):
            continue

        amount_per_unit = _money_to_decimal(money)
        if amount_per_unit is not None and amount_per_unit <= 0:
            amount_per_unit = None
        currency = get_json_value(money, "currency") if isinstance(money, dict) else None
        _upsert_payout_calendar_event(
            db,
            account_id=account_id,
            position=position,
            event_type=event_type,
            event_uid=event_uid,
            payment_date=payment_date,
            record_date=record_date,
            last_buy_date=last_buy_date,
            coupon_start_date=coupon_start_date,
            coupon_end_date=coupon_end_date,
            coupon_period_days=coupon_period_days,
            amount_per_unit=amount_per_unit,
            currency=currency,
            source_event_type=source_event_type,
            fetched_at=fetched_at,
        )
        seen_uids.add(event_uid)
        stored += 1

    stale_query = db.query(PayoutCalendarEvent).filter(
        PayoutCalendarEvent.account_id == account_id,
        PayoutCalendarEvent.figi == position.figi,
        PayoutCalendarEvent.event_type == event_type,
        PayoutCalendarEvent.payment_date >= period_start,
        PayoutCalendarEvent.payment_date <= period_end,
    )
    if seen_uids:
        stale_query = stale_query.filter(~PayoutCalendarEvent.event_uid.in_(seen_uids))
    stale_query.delete(synchronize_session=False)
    return stored


def sync_payout_calendar_for_account(db, account_id: str) -> dict[str, int]:
    latest_snapshot = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.account_id == account_id)
        .order_by(
            PortfolioSnapshot.snapshot_date.desc(),
            PortfolioSnapshot.snapshot_at.desc(),
            PortfolioSnapshot.id.desc(),
        )
        .first()
    )
    if latest_snapshot is None:
        logger.warning(
            "payout_calendar_snapshot_missing",
            "Cannot sync payout calendar without a portfolio snapshot.",
            {"account_id": account_id},
        )
        return {"positions": 0, "events": 0, "failed": 0}

    positions = [
        position
        for position in latest_snapshot.positions
        if position.figi and Decimal(position.quantity or 0) > 0
    ]
    if not positions:
        logger.warning(
            "payout_calendar_positions_missing",
            "Cannot sync payout calendar because the latest snapshot has no positions.",
            {"account_id": account_id, "snapshot_id": latest_snapshot.id},
        )
        return {"positions": 0, "events": 0, "failed": 0}

    period_start = local_today()
    period_end = period_start + timedelta(days=PAYOUT_CALENDAR_HORIZON_DAYS - 1)
    from_iso = dt_to_iso_z(datetime.combine(period_start, datetime.min.time(), tzinfo=LOCAL_TZ))
    dividends_from_iso = dt_to_iso_z(
        datetime.combine(
            period_start - timedelta(days=PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS),
            datetime.min.time(),
            tzinfo=LOCAL_TZ,
        )
    )
    to_iso = dt_to_iso_z(
        datetime.combine(period_end + timedelta(days=1), datetime.min.time(), tzinfo=LOCAL_TZ)
    )
    fetched_at = utc_now_naive()
    held_figis = {position.figi for position in positions}
    events_stored = 0
    failed = 0
    supported_positions = 0

    for position in positions:
        instrument = position.instrument
        instrument_type = _normalize_payout_instrument_type(
            position.instrument_type
            or (instrument.instrument_type if instrument is not None else None)
        )
        instrument_id = position.instrument_uid or position.figi

        if instrument_type == "bond":
            supported_positions += 1
            try:
                source_events = api_get_bond_coupons(instrument_id, from_iso, to_iso)
                events_stored += _replace_position_payout_events(
                    db,
                    account_id=account_id,
                    position=position,
                    event_type="coupon",
                    source_events=source_events,
                    period_start=period_start,
                    period_end=period_end,
                    fetched_at=fetched_at,
                )
            except Exception:
                failed += 1
                logger.exception(
                    "payout_calendar_instrument_sync_failed",
                    "Failed to sync bond coupon calendar; cached rows are preserved.",
                    {
                        "account_id": account_id,
                        "figi": position.figi,
                        "event_type": "coupon",
                    },
                )
        elif instrument_type in {"share", "etf"}:
            supported_positions += 1
            try:
                source_events = api_get_dividends(
                    instrument_id,
                    dividends_from_iso,
                    to_iso,
                )
                events_stored += _replace_position_payout_events(
                    db,
                    account_id=account_id,
                    position=position,
                    event_type="dividend",
                    source_events=source_events,
                    period_start=period_start,
                    period_end=period_end,
                    fetched_at=fetched_at,
                )
            except Exception:
                failed += 1
                logger.exception(
                    "payout_calendar_instrument_sync_failed",
                    "Failed to sync dividend calendar; cached rows are preserved.",
                    {
                        "account_id": account_id,
                        "figi": position.figi,
                        "event_type": "dividend",
                    },
                )

    db.query(PayoutCalendarEvent).filter(
        PayoutCalendarEvent.account_id == account_id,
        PayoutCalendarEvent.payment_date < period_start,
    ).delete(synchronize_session=False)
    db.query(PayoutCalendarEvent).filter(
        PayoutCalendarEvent.account_id == account_id,
        ~PayoutCalendarEvent.figi.in_(held_figis),
    ).delete(synchronize_session=False)
    db.flush()

    return {
        "positions": supported_positions,
        "events": events_stored,
        "failed": failed,
    }


def choose_account(accounts_data: dict) -> dict:
    """
    Выбираем один счёт:
    - если TINKOFF_ACCOUNT_ID задан — по нему;
    - иначе: первый открытый, если есть; иначе просто первый.
    """
    accounts = accounts_data.get("accounts") or []
    if not accounts:
        raise RuntimeError("No accounts returned from API")

    if TINKOFF_ACCOUNT_ID:
        for acc in accounts:
            if str(acc.get("id")) == str(TINKOFF_ACCOUNT_ID):
                return acc

    open_accounts = [a for a in accounts if a.get("status") == "ACCOUNT_STATUS_OPEN"]
    if open_accounts:
        return open_accounts[0]
    return accounts[0]


def ensure_instrument(db, figi: str, instr_data: Optional[dict]) -> Instrument:
    inst: Optional[Instrument] = (
        db.query(Instrument).filter(Instrument.figi == figi).one_or_none()
    )
    if inst is None:
        inst = Instrument(figi=figi)
        db.add(inst)

    if instr_data:
        inst.ticker = instr_data.get("ticker") or inst.ticker
        inst.name = instr_data.get("name") or inst.name
        inst.class_code = instr_data.get("classCode") or inst.class_code
        inst.instrument_type = instr_data.get("instrumentType") or inst.instrument_type

    db.flush()
    return inst


def compute_expected_yield_pct(
    expected_yield: Optional[float],
    position_value: Optional[float],
) -> Optional[float]:
    if expected_yield is None or position_value is None:
        return None
    invested = position_value - expected_yield
    if invested == 0:
        return None
    return expected_yield / invested * 100.0


def get_latest_cost_basis(db, account_id: str, figi: str) -> Optional[float]:
    row = (
        db.query(PortfolioPosition.position_value, PortfolioPosition.expected_yield)
        .join(PortfolioSnapshot, PortfolioSnapshot.id == PortfolioPosition.snapshot_id)
        .filter(
            PortfolioSnapshot.account_id == account_id,
            PortfolioPosition.figi == figi,
        )
        .order_by(PortfolioSnapshot.snapshot_date.desc(), PortfolioSnapshot.snapshot_at.desc())
        .first()
    )
    if not row:
        return None
    position_value, expected_yield = row
    if position_value is None or expected_yield is None:
        return None
    return float(position_value) - float(expected_yield)


def take_snapshot_for_account(db, acc_data: dict):
    """
    Делаем/перезаписываем снапшот за текущий день для одного счёта.
    """
    acc_id = str(acc_data.get("id"))
    acc_name = acc_data.get("name") or "IIS"

    portfolio = api_get_portfolio(acc_id)

    utc_now = utc_now_naive()
    snap_date = local_today()

    total_value = money_to_float(portfolio.get("totalAmountPortfolio"))
    total_shares = money_to_float(portfolio.get("totalAmountShares"))
    total_bonds = money_to_float(portfolio.get("totalAmountBonds"))
    total_etf = money_to_float(portfolio.get("totalAmountEtf"))
    total_currencies = money_to_float(portfolio.get("totalAmountCurrencies"))
    total_futures = money_to_float(portfolio.get("totalAmountFutures"))
    expected_yield = money_to_float(portfolio.get("expectedYield"))

    expected_yield_pct = None
    if total_value is not None and expected_yield is not None:
        invested_portfolio = total_value - expected_yield
        if invested_portfolio != 0:
            expected_yield_pct = expected_yield / invested_portfolio * 100.0

    # Ищем снапшот за этот день по этому счёту
    snap: Optional[PortfolioSnapshot] = (
        db.query(PortfolioSnapshot)
        .filter(
            PortfolioSnapshot.account_id == acc_id,
            PortfolioSnapshot.snapshot_date == snap_date,
        )
        .one_or_none()
    )

    if snap is None:
        snap = PortfolioSnapshot(
            account_id=acc_id,
            account_name=acc_name,
            snapshot_at=utc_now,
            snapshot_date=snap_date,
            currency=PORTFOLIO_CURRENCY.upper(),
        )
        db.add(snap)
        db.flush()
    else:
        # перезаписываем снапшот текущего дня
        snap.account_name = acc_name
        snap.snapshot_at = utc_now
        snap.currency = PORTFOLIO_CURRENCY.upper()
        # удаляем старые позиции
        db.query(PortfolioPosition).filter(
            PortfolioPosition.snapshot_id == snap.id
        ).delete()
        db.flush()

    # обновляем агрегаты
    snap.total_value = total_value
    snap.total_shares = total_shares
    snap.total_bonds = total_bonds
    snap.total_etf = total_etf
    snap.total_currencies = total_currencies
    snap.total_futures = total_futures
    snap.expected_yield = expected_yield
    snap.expected_yield_pct = expected_yield_pct

    db.flush()

    positions = portfolio.get("positions") or []
    figi_cache: dict[str, Optional[dict]] = {}

    for pos in positions:
        figi = pos.get("figi")
        if not figi:
            continue

        # Дёргаем API за метаданными инструмента только если в БД ещё нет ticker/name.
        inst_db = db.query(Instrument).filter(Instrument.figi == figi).one_or_none()
        need_fetch = inst_db is None or not (inst_db.ticker and inst_db.name)

        instr_data = None
        if need_fetch:
            if figi not in figi_cache:
                figi_cache[figi] = api_get_instrument_by_figi(figi)
            instr_data = figi_cache[figi]

        inst = ensure_instrument(db, figi, instr_data)

        quantity = quotation_to_float(pos.get("quantity"))
        current_price = money_to_float(pos.get("currentPrice"))
        current_nkd = money_to_float(get_json_value(pos, "current_nkd"))
        position_value = None
        if quantity is not None and current_price is not None:
            position_value = quantity * current_price

        expected_yield_pos = money_to_float(pos.get("expectedYield"))
        expected_yield_pct_pos = compute_expected_yield_pct(
            expected_yield_pos,
            position_value,
        )

        weight_pct = None
        if position_value is not None and total_value not in (None, 0):
            weight_pct = position_value / total_value * 100.0

        instrument_uid = get_json_value(pos, "instrument_uid")
        position_uid = get_json_value(pos, "position_uid")
        asset_uid = resolve_asset_uid_for_position(
            db,
            asset_uid=get_json_value(pos, "asset_uid"),
            instrument_uid=instrument_uid,
            figi=figi,
        )

        position = PortfolioPosition(
            snapshot_id=snap.id,
            instrument_id=inst.id,
            figi=figi,
            instrument_uid=instrument_uid,
            position_uid=position_uid,
            asset_uid=asset_uid,
            ticker=inst.ticker,
            name=inst.name,
            instrument_type=pos.get("instrumentType"),
            quantity=quantity,
            currency=PORTFOLIO_CURRENCY.upper(),
            current_price=current_price,
            current_nkd=current_nkd,
            position_value=position_value,
            expected_yield=expected_yield_pos,
            expected_yield_pct=expected_yield_pct_pos,
            weight_pct=weight_pct,
            raw_payload_json=json.dumps(pos, ensure_ascii=False, sort_keys=True),
        )
        db.add(position)

    db.flush()

    # Структурированное сообщение о сохранении снапшота
    logger.info(
        "snapshot_saved",
        "Portfolio snapshot saved.",
        {
            "account_id": acc_id,
            "account_name": acc_name,
            "positions": len(positions),
        },
    )


def guess_deposit_source(description: Optional[str]) -> Optional[str]:
    """
    Очень грубая эвристика источника пополнения по description — на будущее.
    """
    if not description:
        return None
    desc = description.lower()
    if "перевод" in desc and "счет" in desc:
        return "transfer"
    if "перевод" in desc and "счёт" in desc:
        return "transfer"
    if "зарплат" in desc:
        return "salary"
    if "пополнени" in desc:
        return "topup"
    return None


def _upsert_operation(db, acc_id: str, op: dict) -> tuple[Optional[Operation], bool]:
    op_id = get_json_value(op, "id")
    if not op_id:
        return None, False

    op_type = get_json_value(op, "type") or get_json_value(op, "operation_type") or "OPERATION_TYPE_UNSPECIFIED"
    payment = get_json_value(op, "payment")
    payment_value = money_to_float(payment) or 0.0
    payment_currency = ((payment or {}).get("currency") or PORTFOLIO_CURRENCY).upper()

    op_dt_raw = parse_iso_dt(get_json_value(op, "date")) or datetime.now(timezone.utc)
    if op_dt_raw.tzinfo is None:
        op_dt = op_dt_raw.replace(tzinfo=timezone.utc).replace(tzinfo=None)
    else:
        op_dt = op_dt_raw.astimezone(timezone.utc).replace(tzinfo=None)

    cancel_dt = parse_iso_dt(get_json_value(op, "cancel_date_time"))
    if cancel_dt and cancel_dt.tzinfo is not None:
        cancel_dt = cancel_dt.astimezone(timezone.utc).replace(tzinfo=None)

    values = {
        "account_id": acc_id,
        "operation_id": op_id,
        "operation_type": op_type,
        "cursor": get_json_value(op, "cursor"),
        "broker_account_id": get_json_value(op, "broker_account_id"),
        "parent_operation_id": get_json_value(op, "parent_operation_id"),
        "name": get_json_value(op, "name"),
        "date": op_dt,
        "state": get_json_value(op, "state"),
        "description": get_json_value(op, "description") or get_json_value(op, "asset_uid") or "",
        "instrument_uid": get_json_value(op, "instrument_uid"),
        "figi": get_json_value(op, "figi"),
        "instrument_type": get_json_value(op, "instrument_type"),
        "instrument_kind": get_json_value(op, "instrument_kind"),
        "position_uid": get_json_value(op, "position_uid"),
        "asset_uid": get_json_value(op, "asset_uid"),
        "amount": payment_value,
        "price": quotation_to_float(get_json_value(op, "price")),
        "commission": money_to_float(get_json_value(op, "commission")),
        "yield_amount": money_to_float(get_json_value(op, "yield")),
        "yield_relative": quotation_to_float(get_json_value(op, "yield_relative")),
        "accrued_int": money_to_float(get_json_value(op, "accrued_int")),
        "quantity": get_json_value(op, "quantity"),
        "quantity_rest": get_json_value(op, "quantity_rest"),
        "quantity_done": get_json_value(op, "quantity_done"),
        "currency": payment_currency,
        "cancel_date_time": cancel_dt,
        "cancel_reason": get_json_value(op, "cancel_reason"),
        "source": guess_deposit_source(get_json_value(op, "description")),
    }

    upsert_asset_alias(
        db,
        asset_uid=values["asset_uid"],
        instrument_uid=values["instrument_uid"],
        figi=values["figi"],
        name=values["name"],
        seen_at=values["date"],
    )

    existing = db.query(Operation).filter(Operation.operation_id == op_id).one_or_none()
    if existing is None:
        operation = Operation(**values)
        db.add(operation)
        return operation, True

    for field, value in values.items():
        setattr(existing, field, value)
    return existing, False


def _sync_operations(
    db,
    account_id: str,
    from_date: Optional[str],
    *,
    affected_income_keys: Optional[set[tuple[str, date, str]]] = None,
) -> dict:
    """Синхронизирует операции счёта через GetOperationsByCursor и upsert в БД."""
    count_new = 0
    count_updated = 0
    loaded_total = 0

    for operations in _iter_operation_pages(account_id, from_date):
        loaded_total += len(operations)

        for op in operations:
            operation, created = _upsert_operation(db, account_id, op)
            if operation is None:
                continue
            if created:
                count_new += 1
            else:
                count_updated += 1
            if (
                affected_income_keys is not None
                and operation.figi
                and operation.operation_type in INCOME_OPERATION_TYPE_MAP
            ):
                event_type, _ = INCOME_OPERATION_TYPE_MAP[operation.operation_type]
                affected_income_keys.add(
                    (operation.figi, operation.date.date(), event_type)
                )

        logger.info(
            "operations_page_persisted",
            "Persisted operations page in the current transaction.",
            {
                "account_id": account_id,
                "page_items_count": len(operations),
                "loaded_total": loaded_total,
            },
        )

    return {"loaded": loaded_total, "new": count_new, "updated": count_updated}


def sync_operations(account_id: str, from_date: Optional[str]) -> dict:
    """Отдельная функция синхронизации операций: API + курсор + сохранение в БД."""
    with SessionLocal() as db:
        stats = _sync_operations(db, account_id, from_date)
        db.commit()
        return stats


INCOME_OPERATION_TYPE_MAP = {
    "OPERATION_TYPE_COUPON": ("coupon", "gross"),
    # COUPON_TAX сохраняется для совместимости с уже загруженными данными.
    "OPERATION_TYPE_COUPON_TAX": ("coupon", "tax"),
    "OPERATION_TYPE_BOND_TAX": ("coupon", "tax"),
    "OPERATION_TYPE_BOND_TAX_PROGRESSIVE": ("coupon", "tax"),
    "OPERATION_TYPE_DIVIDEND": ("dividend", "gross"),
    "OPERATION_TYPE_DIVIDEND_TAX": ("dividend", "tax"),
    "OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE": ("dividend", "tax"),
}
EXECUTED_OPERATION_STATE = "OPERATION_STATE_EXECUTED"


def _reconcile_income_events(
    db,
    account_id: str,
    affected_keys: set[tuple[str, date, str]],
) -> dict:
    """
    Пересчитывает затронутые доходные события по полной локальной истории.

    Полная история ключа нужна для поздних налогов: узкое
    API-окно может содержать налог, но не исходную выплату.
    """
    if not affected_keys:
        return {"income_created": 0, "income_updated": 0}

    income_by_key: dict[tuple[str, date, str], dict[str, float]] = {}

    rows = (
        db.query(Operation)
        .filter(
            Operation.account_id == account_id,
            Operation.state == EXECUTED_OPERATION_STATE,
            Operation.operation_type.in_(tuple(INCOME_OPERATION_TYPE_MAP)),
        )
        .all()
    )
    for row in rows:
        if not row.figi:
            continue
        event_type, amount_kind = INCOME_OPERATION_TYPE_MAP[row.operation_type]
        key = (row.figi, row.date.date(), event_type)
        if key not in affected_keys:
            continue
        if key not in income_by_key:
            income_by_key[key] = {"gross": 0.0, "tax": 0.0}
        income_by_key[key][amount_kind] += float(row.amount or 0)

    existing_events = (
        db.query(IncomeEvent)
        .filter(IncomeEvent.account_id == account_id)
        .all()
    )
    existing_by_key = {
        (row.figi, row.event_date, row.event_type): row
        for row in existing_events
        if (row.figi, row.event_date, row.event_type) in affected_keys
    }

    cost_basis_by_figi: dict[str, Optional[float]] = {}
    created = 0
    updated = 0

    for (figi, event_date, event_type), amounts in income_by_key.items():
        gross_sum = amounts["gross"]
        tax_sum = amounts["tax"]
        net_amount = compute_income_net_amount(gross_sum, tax_sum)
        if net_amount <= 0:
            continue

        expected_amounts = {
            "gross_amount": round(gross_sum, 2),
            "tax_amount": round(tax_sum, 2),
            "net_amount": round(net_amount, 2),
        }
        key = (figi, event_date, event_type)
        existing = existing_by_key.get(key)
        if existing is None:
            if figi not in cost_basis_by_figi:
                cost_basis_by_figi[figi] = get_latest_cost_basis(db, account_id, figi)
            net_yield_pct = compute_income_net_yield_pct(
                net_amount,
                cost_basis_by_figi[figi],
            )
            db.add(
                IncomeEvent(
                    account_id=account_id,
                    figi=figi,
                    event_date=event_date,
                    event_type=event_type,
                    notified=False,
                    net_yield_pct=round(net_yield_pct, 4),
                    **expected_amounts,
                )
            )
            created += 1
            continue

        amounts_changed = any(
            round(float(getattr(existing, field)), 2) != value
            for field, value in expected_amounts.items()
        )
        if amounts_changed:
            if figi not in cost_basis_by_figi:
                cost_basis_by_figi[figi] = get_latest_cost_basis(db, account_id, figi)
            net_yield_pct = compute_income_net_yield_pct(
                net_amount,
                cost_basis_by_figi[figi],
            )
            for field, value in expected_amounts.items():
                setattr(existing, field, value)
            existing.net_yield_pct = round(net_yield_pct, 4)
            # Уже отправленное уведомление не повторяем; отчёты и dataset
            # сразу увидят исправленные суммы после commit.
            updated += 1

    return {
        "income_created": created,
        "income_updated": updated,
    }


def sync_operations_for_account(db, acc_data: dict):
    """Тянем операции и сохраняем в operations (идемпотентно по operation_id)."""
    acc_id = str(acc_data.get("id"))
    opened_iso = acc_data.get("openedDate") or acc_data.get("opened_date")

    last_dt: Optional[datetime] = (
        db.query(func.max(Operation.date))
        .filter(Operation.account_id == acc_id)
        .scalar()
    )

    from_iso = opened_iso
    if last_dt is not None:
        from_iso = dt_to_iso_z(last_dt - timedelta(days=1))

    # После расширения схемы OperationItem может потребоваться дозаполнение
    # новых колонок у исторических строк. Если видим пустые новые поля,
    # делаем backfill с даты открытия счёта.
    needs_backfill = (
        db.query(Operation.id)
        .filter(
            Operation.account_id == acc_id,
            Operation.state.is_(None),
        )
        .first()
        is not None
    )
    if needs_backfill and opened_iso:
        from_iso = opened_iso
        logger.info(
            "operations_backfill_started",
            "Detected incomplete OperationItem fields; starting backfill from account open date.",
            {"account_id": acc_id, "from": from_iso},
        )

    affected_income_keys: set[tuple[str, date, str]] = set()
    stats = _sync_operations(
        db,
        acc_id,
        from_iso,
        affected_income_keys=affected_income_keys,
    )
    stats.update(
        _reconcile_income_events(
            db,
            acc_id,
            affected_income_keys,
        )
    )

    logger.info(
        "operations_sync_completed",
        "Operations sync completed.",
        {"account_id": acc_id, **stats},
    )


def run_snapshot_and_operations_once():
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


def job_with_retry():
    """
    Обёртка для планировщика:
    - одна попытка на запуск;
    - без sleep() внутри;
    - при ошибке всё сделает следующий запуск по расписанию.
    """
    try:
        logger.info("snapshot_job_started", "Snapshot job started.")
        run_snapshot_and_operations_once()
        logger.info("snapshot_job_completed", "Snapshot job completed successfully.")
    except Exception:
        logger.exception("snapshot_job_failed", "Snapshot job failed.")


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


def main() -> int:
    if not API_TOKEN:
        logger.error(
            "missing_api_token",
            "TINVEST_API_TOKEN не задан. Передай его через переменную окружения.",
        )
        return 1

    init_db()

    # Разовый запуск при старте — перезаписываем текущий день
    job_with_retry()
    payout_calendar_job_with_retry()

    # Планировщик: запускаем job_with_retry каждые SNAPSHOT_INTERVAL_MINUTES минут
    scheduler = BlockingScheduler(timezone=SCHED_TZ)
    scheduler.add_job(
        payout_calendar_job_with_retry,
        CronTrigger(
            hour=PAYOUT_CALENDAR_SYNC_HOUR,
            minute=PAYOUT_CALENDAR_SYNC_MINUTE,
            timezone=LOCAL_TZ,
        ),
        name="daily_payout_calendar_sync",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    logger.info(
        "payout_calendar_schedule_registered",
        "Payout calendar schedule registered.",
        {
            "hour": PAYOUT_CALENDAR_SYNC_HOUR,
            "minute": PAYOUT_CALENDAR_SYNC_MINUTE,
            "timezone": SCHED_TZ,
            "horizon_days": PAYOUT_CALENDAR_HORIZON_DAYS,
        },
    )

    if SNAPSHOT_MODE == "cron":
        trigger = CronTrigger(hour=SNAPSHOT_HOUR, minute=SNAPSHOT_MINUTE)
        scheduler.add_job(
            job_with_retry,
            trigger,
            name="daily_snapshot",
            misfire_grace_time=3600,
            replace_existing=True,
        )
        logger.info(
            "scheduler_started",
            "Scheduler started in cron mode.",
            {
                "mode": "cron",
                "snapshot_hour": SNAPSHOT_HOUR,
                "snapshot_minute": SNAPSHOT_MINUTE,
                "timezone": SCHED_TZ,
            },
        )
    else:
        trigger = IntervalTrigger(minutes=SNAPSHOT_INTERVAL_MINUTES)
        scheduler.add_job(
            job_with_retry,
            trigger,
            name="interval_snapshot",
            misfire_grace_time=SNAPSHOT_INTERVAL_MINUTES * 60,
            replace_existing=True,
        )
        logger.info(
            "scheduler_started",
            "Scheduler started in interval mode.",
            {
                "mode": "interval",
                "snapshot_interval_minutes": SNAPSHOT_INTERVAL_MINUTES,
                "timezone": SCHED_TZ,
            },
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("service_stopped", "Service stopped.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "tracker_process_failed",
            "Tracker process terminated with an unhandled exception.",
        )
        raise SystemExit(1)
