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
    String,
    Date,
    DateTime,
    Numeric,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Import unified JSON logging setup
from common.logging_setup import configure_logging, get_logger

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
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)

    quantity = Column(Numeric(18, 6), nullable=True)
    currency = Column(String, nullable=True)

    current_price = Column(Numeric(18, 4), nullable=True)
    position_value = Column(Numeric(18, 2), nullable=True)
    expected_yield = Column(Numeric(18, 2), nullable=True)
    expected_yield_pct = Column(Numeric(9, 4), nullable=True)
    weight_pct = Column(Numeric(9, 4), nullable=True)

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
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    currency = Column(String, nullable=False)

    description = Column(String, nullable=True)
    source = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


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

        position = PortfolioPosition(
            snapshot_id=snap.id,
            instrument_id=inst.id,
            figi=figi,
            ticker=inst.ticker,
            name=inst.name,
            instrument_type=pos.get("instrumentType"),
            quantity=quantity,
            currency=PORTFOLIO_CURRENCY.upper(),
            current_price=current_price,
            position_value=position_value,
            expected_yield=expected_yield_pos,
            expected_yield_pct=expected_yield_pct_pos,
            weight_pct=weight_pct,
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


def sync_operations_for_account(db, acc_data: dict):
    """
    Тянем все операции и кладём их в operations (без дублей).
    Для обратной совместимости старые SQL-запросы читают пополнения через view deposits.
    """
    acc_id = str(acc_data.get("id"))
    opened_iso = acc_data.get("openedDate") or acc_data.get("opened_date")

    # Инкрементальная синхронизация: начинаем не с открытия счёта, а с последней
    # сохранённой операции (минус 1 день для страховки от задержек/часовых поясов).
    last_dt: Optional[datetime] = (
        db.query(func.max(Operation.date))
        .filter(
            Operation.account_id == acc_id,
        )
        .scalar()
    )

    from_iso = opened_iso
    if last_dt is not None:
        from_dt = (last_dt - timedelta(days=1))
        from_iso = dt_to_iso_z(from_dt)

    count_new = 0
    total_amount = 0.0
    currency_seen: Optional[str] = None
    type_breakdown: dict[str, dict[str, float | int]] = {}

    for op in api_get_operations_by_cursor(acc_id, from_iso):
        op_type = op.get("operationType") or op.get("type")
        payment = op.get("payment")
        val = money_to_float(payment)
        if val is None:
            continue

        op_currency = (payment.get("currency") or PORTFOLIO_CURRENCY).upper()
        currency_seen = currency_seen or op_currency

        op_id = op.get("id")
        op_date_str = op.get("date")
        op_dt_raw = parse_iso_dt(op_date_str)
        if op_dt_raw is None:
            op_dt_raw = datetime.now(timezone.utc)
        # В БД храним naive UTC
        if op_dt_raw.tzinfo is None:
            op_dt = op_dt_raw.replace(tzinfo=timezone.utc).replace(tzinfo=None)
        else:
            op_dt = op_dt_raw.astimezone(timezone.utc).replace(tzinfo=None)
        desc = op.get("description") or op.get("assetUid") or ""

        exists = (
            db.query(Operation)
            .filter(
                Operation.account_id == acc_id,
                Operation.operation_id == op_id,
            )
            .one_or_none()
        )
        if exists:
            continue

        src = guess_deposit_source(desc)

        operation = Operation(
            account_id=acc_id,
            operation_id=op_id,
            operation_type=op_type or "OPERATION_TYPE_UNSPECIFIED",
            date=op_dt,
            amount=val,
            currency=op_currency,
            description=desc,
            source=src,
        )
        db.add(operation)
        count_new += 1
        total_amount += val
        operation_type = operation.operation_type
        if operation_type not in type_breakdown:
            type_breakdown[operation_type] = {"count": 0, "sum": 0.0}
        type_breakdown[operation_type]["count"] += 1
        type_breakdown[operation_type]["sum"] += val

    db.flush()

    if currency_seen is None:
        currency_seen = PORTFOLIO_CURRENCY.upper()

    # Логируем результаты синхронизации
    logger.info(
        "operations_sync",
        f"Operations sync for account {acc_id}: new records={count_new}, sum={total_amount:.2f} {currency_seen}",
        extra={
            "ctx": {
                "account_id": acc_id,
                "new_records": count_new,
                "sum": round(total_amount, 2),
                "currency": currency_seen,
                "type_breakdown": {
                    k: {"count": v["count"], "sum": round(v["sum"], 2)}
                    for k, v in type_breakdown.items()
                },
            }
        },
    )


def sync_deposits_for_account(db, acc_data: dict):
    """Deprecated wrapper for backward compatibility."""
    return sync_operations_for_account(db, acc_data)


def run_snapshot_and_operations_once():
    accounts_data = api_get_accounts()
    acc = choose_account(accounts_data)

    with SessionLocal() as db:
        # 1) Снапшот не должен зависеть от синка операций.
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
