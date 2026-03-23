"""
iis_tracker: ежедневные снапшоты портфеля + таблица пополнений
для одного T-Invest счёта (ИИС).

Функционал:
- при старте: один раз делаем снапшот за текущий день (перезапись дня);
- дальше: по расписанию (каждые SNAPSHOT_INTERVAL_MINUTES минут) делаем снапшот;
- для каждого дня:
    * сохраняем агрегаты по портфелю;
    * сохраняем состав портфеля (позиции);
    * синхронизируем операции в таблицу operations (для совместимости deposits читается через view).
"""

import os
import sys
import json
import textwrap
import traceback
from datetime import datetime, timezone, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo
import requests
import urllib3
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

# ============ CONFIG из окружения ============

# Токен T-Invest. Лучше передавать через ENV, но можно и захардкодить здесь.
API_TOKEN = os.getenv("TINVEST_API_TOKEN", "").strip()
if not API_TOKEN:
    # Логируем ошибку и падаем, как и раньше.
    logger.error(
        "missing_api_token",
        "TINVEST_API_TOKEN не задан. Передай его через переменную окружения.",
    )
    raise RuntimeError("TINVEST_API_TOKEN не задан. Передай его через переменную окружения.")

BASE_URL = os.getenv(
    "TINVEST_BASE_URL",
    "https://invest-public-api.tbank.ru/rest",
)

ACCOUNT_STATUS = os.getenv("TINVEST_ACCOUNT_STATUS", "ACCOUNT_STATUS_ALL")
PORTFOLIO_CURRENCY = os.getenv("TINVEST_PORTFOLIO_CURRENCY", "RUB")

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


class Deposit(Base):
    __tablename__ = "deposits"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "operation_id",
            name="uq_deposits_account_operation",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)  # ID счёта в Т-Инвест

    operation_id = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    currency = Column(String, nullable=False)

    description = Column(String, nullable=True)
    source = Column(String, nullable=True)  # простая попытка классифицировать

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


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

    seen_at = seen_at or datetime.utcnow()
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
    alias.updated_at = datetime.utcnow()


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


def post_api(method_path: str, payload: dict) -> dict:
    url = f"{BASE_URL}/{method_path}"

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=20,
            verify=VERIFY_SSL,
        )
    except requests.exceptions.SSLError as e:
        # Логируем, но не логируем тело/заголовки
        logger.error("ssl_error", f"SSL error: {e}", exc_info=True)
        raise
    except requests.exceptions.RequestException as e:
        logger.error("http_error", f"HTTP error: {e}", exc_info=True)
        raise

    if resp.status_code != 200:
        logger.error(
            "api_http_error",
            f"API HTTP error: {resp.status_code}",
            extra={"ctx": {"url_host": url.split('//')[1].split('/')[0], "path": '/' + '/'.join(url.split('/')[3:]), "status_code": resp.status_code}},
        )
        # Пытаемся вывести тело для отладки, но через stderr — оставляем прежнее поведение
        try:
            sys.stderr.write(json.dumps(resp.json(), ensure_ascii=False) + "\n")
        except Exception:
            sys.stderr.write(resp.text + "\n")
        raise RuntimeError(f"API HTTP {resp.status_code}")

    try:
        return resp.json()
    except json.JSONDecodeError:
        logger.error("json_decode_error", "JSON decode error")
        # выводим текст в stderr для диагностики
        sys.stderr.write(resp.text + "\n")
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


def api_get_instrument_by_figi(figi: str) -> Optional[dict]:
    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy",
        {
            "idType": "INSTRUMENT_ID_TYPE_FIGI",
            "id": figi,
        },
    )
    return data.get("instrument")


def api_get_operations_by_cursor(account_id: str, opened_iso: Optional[str]):
    """
    Генератор по всем операциям счёта.
    """
    if opened_iso:
        from_dt = opened_iso
    else:
        from_dt = "2000-01-01T00:00:00Z"

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cursor = ""
    # Защита от бесконечного цикла: иногда API может вернуть повторяющийся cursor/nextCursor.
    seen_cursors: set[str] = set()
    max_pages = int(os.getenv("OPERATIONS_MAX_PAGES", "10000"))
    page_i = 0
    while True:
        if cursor and cursor in seen_cursors:
            logger.warning(
                "operations_cursor_repeated",
                "Operations cursor повторился — прерываю цикл, чтобы не зависнуть.",
            )
            break
        if cursor:
            seen_cursors.add(cursor)

        payload = {
            "accountId": account_id,
            "from": from_dt,
            "to": now_iso,
            "cursor": cursor,
            "limit": 1000,
            "withoutTrades": True,
        }
        data = post_api(
            "tinkoff.public.invest.api.contract.v1.OperationsService/GetOperationsByCursor",
            payload,
        )
        operations = data.get("items") or data.get("operations") or []
        for op in operations:
            yield op

        has_next = data.get("hasNext", False)
        next_cursor = data.get("nextCursor") or ""
        logger.info("operations_page_loaded", f"loaded operations: {len(operations)}")
        logger.info("operations_next_cursor", f"next_cursor: {next_cursor}")

        # Следующая страница
        if not has_next or not next_cursor:
            break

        page_i += 1
        if page_i >= max_pages:
            logger.warning(
                "operations_max_pages_reached",
                "OPERATIONS_MAX_PAGES достигнут — прерываю синхронизацию операций.",
            )
            break

        cursor = next_cursor


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


def get_latest_cost_basis(db, figi: str) -> Optional[float]:
    row = (
        db.query(PortfolioPosition.position_value, PortfolioPosition.expected_yield)
        .join(PortfolioSnapshot, PortfolioSnapshot.id == PortfolioPosition.snapshot_id)
        .filter(PortfolioPosition.figi == figi)
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
        f"Snapshot saved for account {acc_name} ({acc_id}), positions: {len(positions)}",
        extra={"ctx": {"account_id": acc_id, "positions": len(positions)}},
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


def _sync_operations(db, account_id: str, from_date: Optional[str]) -> dict:
    """Синхронизирует операции счёта через GetOperationsByCursor и upsert в БД."""
    cursor = ""
    count_new = 0
    count_updated = 0
    loaded_total = 0

    while True:
        payload = {
            "accountId": account_id,
            "from": from_date or "2000-01-01T00:00:00Z",
            "to": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 1000,
            "withoutTrades": True,
            "cursor": cursor,
        }
        data = post_api(
            "tinkoff.public.invest.api.contract.v1.OperationsService/GetOperationsByCursor",
            payload,
        )

        operations = data.get("items") or data.get("operations") or []
        loaded_total += len(operations)

        for op in operations:
            operation, created = _upsert_operation(db, account_id, op)
            if operation is None:
                continue
            if created:
                count_new += 1
            else:
                count_updated += 1

        next_cursor = data.get("nextCursor") or ""
        logger.info("operations_page_loaded", f"loaded operations: {loaded_total}")
        logger.info("operations_next_cursor", f"next_cursor: {next_cursor}")

        has_next = bool(data.get("hasNext"))
        if not has_next or not next_cursor:
            break
        cursor = next_cursor

    return {"loaded": loaded_total, "new": count_new, "updated": count_updated}


def sync_operations(account_id: str, from_date: Optional[str]) -> dict:
    """Отдельная функция синхронизации операций: API + курсор + сохранение в БД."""
    with SessionLocal() as db:
        stats = _sync_operations(db, account_id, from_date)
        db.commit()
        return stats


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
            f"Detected incomplete OperationItem fields for account {acc_id}; backfill from account open date.",
            extra={"ctx": {"account_id": acc_id, "from": from_iso}},
        )

    stats = _sync_operations(db, acc_id, from_iso)

    income_type_map = {
        "OPERATION_TYPE_COUPON": ("coupon", "gross"),
        "OPERATION_TYPE_COUPON_TAX": ("coupon", "tax"),
        "OPERATION_TYPE_DIVIDEND": ("dividend", "gross"),
        "OPERATION_TYPE_DIVIDEND_TAX": ("dividend", "tax"),
    }
    income_by_key: dict[tuple[str, date, str], dict[str, float]] = {}
    from_dt = parse_iso_dt(from_iso) if from_iso else None

    query = db.query(Operation).filter(Operation.account_id == acc_id)
    if from_dt is not None:
        if from_dt.tzinfo is not None:
            from_dt = from_dt.astimezone(timezone.utc).replace(tzinfo=None)
        query = query.filter(Operation.date >= from_dt)

    for row in query:
        if not row.figi or row.operation_type not in income_type_map:
            continue
        event_type, amount_kind = income_type_map[row.operation_type]
        key = (row.figi, row.date.date(), event_type)
        if key not in income_by_key:
            income_by_key[key] = {"gross": 0.0, "tax": 0.0}
        income_by_key[key][amount_kind] += float(row.amount or 0)

    for (figi, event_date, event_type), amounts in income_by_key.items():
        gross_sum = amounts["gross"]
        tax_sum = amounts["tax"]
        net_amount = compute_income_net_amount(gross_sum, tax_sum)
        if net_amount <= 0:
            continue

        cost_basis = get_latest_cost_basis(db, figi)
        net_yield_pct = compute_income_net_yield_pct(net_amount, cost_basis)

        db.execute(
            text(
                """
                INSERT INTO income_events (
                    account_id, figi, event_date, event_type,
                    gross_amount, tax_amount, net_amount, net_yield_pct, notified
                ) VALUES (
                    :account_id, :figi, :event_date, :event_type,
                    :gross_amount, :tax_amount, :net_amount, :net_yield_pct, false
                )
                ON CONFLICT (account_id, figi, event_date, event_type) DO NOTHING
                """
            ),
            {
                "account_id": acc_id,
                "figi": figi,
                "event_date": event_date,
                "event_type": event_type,
                "gross_amount": round(gross_sum, 2),
                "tax_amount": round(tax_sum, 2),
                "net_amount": round(net_amount, 2),
                "net_yield_pct": round(net_yield_pct, 4),
            },
        )

    logger.info(
        "operations_sync",
        f"Operations sync for account {acc_id}: new records={stats['new']}, updated records={stats['updated']}, loaded={stats['loaded']}",
        extra={"ctx": {"account_id": acc_id, **stats}},
    )

def sync_deposits_for_account(db, acc_data: dict):
    """Deprecated wrapper for backward compatibility."""
    return sync_operations_for_account(db, acc_data)


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
            logger.exception("operations_sync_failed", "Operations sync failed (snapshot сохранён).")


def job_with_retry():
    """
    Обёртка для планировщика:
    - одна попытка на запуск;
    - без sleep() внутри;
    - при ошибке всё сделает следующий запуск по расписанию.
    """
    try:
        logger.info("snapshot_job_start", "Snapshot job started.")
        run_snapshot_and_operations_once()
        logger.info("snapshot_job_completed", "Snapshot job completed successfully.")
    except Exception as e:
        logger.exception("snapshot_job_failed", f"Snapshot job failed: {e}")


def main():
    init_db()

    # Разовый запуск при старте — перезаписываем текущий день
    job_with_retry()

    # Планировщик: запускаем job_with_retry каждые SNAPSHOT_INTERVAL_MINUTES минут
    scheduler = BlockingScheduler(timezone=SCHED_TZ)

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
            f"Scheduler started. Daily snapshot at {SNAPSHOT_HOUR:02d}:{SNAPSHOT_MINUTE:02d} ({SCHED_TZ}).",
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
            f"Scheduler started. Snapshot every {SNAPSHOT_INTERVAL_MINUTES} minutes ({SCHED_TZ}).",
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("service_stopped", "Service stopped.")


if __name__ == "__main__":
    main()
