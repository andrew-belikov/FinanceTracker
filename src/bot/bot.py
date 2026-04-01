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

- Ежедневная задача (в заданное время JobQueue по TIMEZONE, через JobQueue):
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

import os
import random
import tempfile
import csv
import json
import zipfile
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

from telegram import Update, InputFile
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Импорт шаблонов
from today_templates import TodayContext, render_today_text
from week_templates import WeekContext, render_week_text
from month_templates import MonthContext, render_month_text

# ================= CONFIG =================

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

ALLOWED_USER_IDS_STR = os.getenv("ALLOWED_USER_IDS", "365469")
ALLOWED_USER_IDS = {
    int(x.strip()) for x in ALLOWED_USER_IDS_STR.split(",") if x.strip()
}

# В какие чаты слать авто-отчёты. Для личных чатов chat_id == user_id,
# так что можно использовать тот же список.
TARGET_CHAT_IDS = ALLOWED_USER_IDS

# Название счёта в текстах
ACCOUNT_FRIENDLY_NAME = os.getenv("ACCOUNT_FRIENDLY_NAME", "Семейный капитал")

# Таймзона для отображения дат в тексте и расписания JobQueue
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
TZ = ZoneInfo(TZ_NAME)

DAILY_JOB_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", "18"))
DAILY_JOB_MINUTE = int(os.getenv("DAILY_SUMMARY_MINUTE", "0"))


def build_daily_job_time() -> time:
    return time(
        hour=DAILY_JOB_HOUR,
        minute=DAILY_JOB_MINUTE,
        tzinfo=TZ,
    )


def format_daily_job_schedule() -> str:
    return f"{DAILY_JOB_HOUR:02d}:{DAILY_JOB_MINUTE:02d} ({TZ_NAME})"


DAILY_JOB_SCHEDULE_LABEL = format_daily_job_schedule()

# Одноразовый тест JobQueue при старте (для валидации отправки).
JOBQUEUE_SMOKE_TEST_ON_START = (
    os.getenv("JOBQUEUE_SMOKE_TEST_ON_START", "false").strip().lower() in {"1", "true", "yes", "on"}
)
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS = int(os.getenv("JOBQUEUE_SMOKE_TEST_DELAY_SECONDS", "20"))

BOT_PROXY_ENABLED = (
    os.getenv("BOT_PROXY_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
BOT_PROXY_ENDPOINT = os.getenv("BOT_PROXY_ENDPOINT", "http://xray-client:3128").strip()

TELEGRAM_REQUEST_CONNECT_TIMEOUT_SECONDS = 20.0
TELEGRAM_REQUEST_READ_TIMEOUT_SECONDS = 20.0
TELEGRAM_REQUEST_WRITE_TIMEOUT_SECONDS = 20.0
TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS = 30.0
TELEGRAM_REQUEST_CONNECTION_POOL_SIZE = 256

TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS = 60
TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT_SECONDS = 20.0
TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS = 75.0
TELEGRAM_GET_UPDATES_WRITE_TIMEOUT_SECONDS = 20.0
TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS = 30.0
TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE = 2
TELEGRAM_POLL_INTERVAL_SECONDS = 0.0
POLLING_WATCHDOG_INTERVAL_SECONDS = 60
POLLING_BACKLOG_PENDING_THRESHOLD = 1
POLLING_BACKLOG_STALL_THRESHOLD_SECONDS = 180

BOT_PROCESS_STARTED_AT_UTC = datetime.now(timezone.utc)
LAST_UPDATE_RECEIVED_AT_UTC: datetime | None = None
POLLING_BACKLOG_ACTIVE = False


def resolve_telegram_proxy_url() -> str | None:
    if not BOT_PROXY_ENABLED:
        return None
    return BOT_PROXY_ENDPOINT or None


def build_telegram_request_kwargs(
    *,
    proxy_url: str | None,
    connection_pool_size: int,
    connect_timeout: float,
    read_timeout: float,
    write_timeout: float,
    pool_timeout: float,
) -> dict:
    kwargs = {
        "connection_pool_size": connection_pool_size,
        "connect_timeout": connect_timeout,
        "read_timeout": read_timeout,
        "write_timeout": write_timeout,
        "pool_timeout": pool_timeout,
        "http_version": "1.1",
        # Avoid implicit proxy/env surprises; proxy, if needed, is configured explicitly.
        "httpx_kwargs": {"trust_env": False},
    }
    if proxy_url is not None:
        kwargs["proxy"] = proxy_url
    return kwargs


def build_bot_application() -> Application:
    proxy_url = resolve_telegram_proxy_url()
    request = HTTPXRequest(
        **build_telegram_request_kwargs(
            proxy_url=proxy_url,
            connection_pool_size=TELEGRAM_REQUEST_CONNECTION_POOL_SIZE,
            connect_timeout=TELEGRAM_REQUEST_CONNECT_TIMEOUT_SECONDS,
            read_timeout=TELEGRAM_REQUEST_READ_TIMEOUT_SECONDS,
            write_timeout=TELEGRAM_REQUEST_WRITE_TIMEOUT_SECONDS,
            pool_timeout=TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS,
        )
    )
    get_updates_request = HTTPXRequest(
        **build_telegram_request_kwargs(
            proxy_url=proxy_url,
            connection_pool_size=TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE,
            connect_timeout=TELEGRAM_GET_UPDATES_CONNECT_TIMEOUT_SECONDS,
            read_timeout=TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS,
            write_timeout=TELEGRAM_GET_UPDATES_WRITE_TIMEOUT_SECONDS,
            pool_timeout=TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS,
        )
    )
    return (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .build()
    )


def is_polling_backlog_detected(
    *,
    pending_update_count: int,
    last_update_received_at: datetime | None,
    process_started_at: datetime,
    now_utc: datetime,
    pending_threshold: int = POLLING_BACKLOG_PENDING_THRESHOLD,
    stall_threshold_seconds: int = POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
) -> bool:
    reference_dt = last_update_received_at or process_started_at
    stall_duration_seconds = (now_utc - reference_dt).total_seconds()
    return pending_update_count >= pending_threshold and stall_duration_seconds >= stall_threshold_seconds

# Годовой план пополнений
PLAN_ANNUAL_CONTRIB_RUB = float(os.getenv("PLAN_ANNUAL_CONTRIB_RUB", "400000"))
TINKOFF_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID", "auto").strip()

# Подключение к БД (та же, что у сервиса снапшотов)
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "fintracker")
DB_USER = os.getenv("DB_USER", "aqua4")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Q1a2z334")

DB_DSN = os.getenv(
    "DB_DSN",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

# Русские названия месяцев
MONTHS_RU = {
    1: "январь",
    2: "февраль",
    3: "март",
    4: "апрель",
    5: "май",
    6: "июнь",
    7: "июль",
    8: "август",
    9: "сентябрь",
    10: "октябрь",
    11: "ноябрь",
    12: "декабрь",
}

MONTHS_RU_GENITIVE = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}

SHORT_MONTHS_RU = {
    1: "янв",
    2: "фев",
    3: "мар",
    4: "апр",
    5: "май",
    6: "июн",
    7: "июл",
    8: "авг",
    9: "сен",
    10: "окт",
    11: "ноя",
    12: "дек",
}

CHART_COLORS = {
    "portfolio": "#1f6f8b",
    "portfolio_fill": "#d9eef4",
    "deposits": "#d8a25e",
    "deposits_fill": "#f6e4c9",
    "twr": "#6b7aa1",
    "positive": "#3e8e63",
    "positive_fill": "#dcefe3",
    "negative": "#c46b4f",
    "negative_fill": "#f5dfd7",
    "neutral": "#8f97a6",
    "grid": "#d8dfe7",
    "spine": "#d6dce3",
    "text": "#1f2933",
    "muted": "#67707b",
}

# ==========================================

# Structured logging configuration
from common.logging_setup import configure_logging, get_logger
from common.text_utils import has_mojibake

# Configure logging once at module load
configure_logging()
logger = get_logger("iis_tracker_bot")

engine = create_engine(DB_DSN, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

DEPOSIT_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_INPUT",)
COMMISSION_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_BROKER_FEE",
    "OPERATION_TYPE_MARGIN_FEE",
    "OPERATION_TYPE_SUCCESS_FEE",
    "OPERATION_TYPE_WITHDRAW_COMMISSION",
    "OPERATION_TYPE_OTHER_FEE",
)
TAX_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_TAX",
    "OPERATION_TYPE_TAX_PROGRESSIVE",
    "OPERATION_TYPE_TAX_COUPON",
    "OPERATION_TYPE_TAX_DIVIDEND",
)
INCOME_EVENT_TAX_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_COUPON_TAX",
    "OPERATION_TYPE_DIVIDEND_TAX",
)
INCOME_TAX_OPERATION_TYPES: tuple[str, ...] = TAX_OPERATION_TYPES + INCOME_EVENT_TAX_OPERATION_TYPES
INCOME_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_COUPON",
    "OPERATION_TYPE_DIVIDEND",
)
WITHDRAWAL_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_OUTPUT",)
BUY_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_BUY",
    "OPERATION_TYPE_BUY_CARD",
)
SELL_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_SELL",)
EXECUTED_OPERATION_STATE = "OPERATION_STATE_EXECUTED"

OPERATIONS_DEDUP_CTE = """
WITH operations_dedup AS (
    SELECT DISTINCT ON (account_id, COALESCE(operation_id, id::text))
        id,
        account_id,
        operation_id,
        date,
        amount,
        operation_type,
        state,
        figi,
        name,
        commission,
        yield
    FROM operations
    ORDER BY account_id, COALESCE(operation_id, id::text), id DESC
)
"""

YEAR_REPORT_TOP_N = 5
SNAPSHOT_TOTAL_FIELDS = {
    "share": "total_shares",
    "bond": "total_bonds",
    "etf": "total_etf",
    "currency": "total_currencies",
    "futures": "total_futures",
    "future": "total_futures",
}

REBALANCE_ASSET_CLASSES: tuple[str, ...] = ("stocks", "bonds", "etf", "currency")
REBALANCE_TARGET_ALIASES = {
    "stocks": "stocks",
    "bonds": "bonds",
    "etf": "etf",
    "cash": "currency",
    "currency": "currency",
}
REBALANCE_CLASS_LABELS = {
    "stocks": "Акции",
    "bonds": "Облигации",
    "etf": "ETF",
    "currency": "Валюта",
}
REBALANCE_GROUP_TO_CLASS = {
    "Акции": "stocks",
    "Облигации": "bonds",
    "ETF": "etf",
    "Валюта": "currency",
}
REBALANCE_TOLERANCE_PCT = Decimal("5.0")
REBALANCE_FEATURE_UNAVAILABLE_TEXT = (
    "Функция таргетов пока недоступна: таблицы ещё не созданы. "
    "Перезапустите tracker и bot или примените миграцию, затем попробуйте снова."
)
REBALANCE_TARGETS_NOT_CONFIGURED_TEXT = (
    "Таргеты пока не настроены.\n\n"
    "Пример: `/targets set stocks=50 bonds=30 cash=20`"
)
TARGETS_USAGE_TEXT = "Формат: /targets или /targets set stocks=50 bonds=30 cash=20"
INVEST_USAGE_TEXT = "Формат: /invest 30000"


@contextmanager
def db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


async def safe_send_message(bot, chat_id: int, text: str, parse_mode: str = "Markdown"):
    """Send message; if Markdown parsing fails, fallback to plain text."""
    try:
        logger.info(
            "bot_send_message_started",
            "Sending Telegram message.",
            {"chat_id": chat_id, "parse_mode": parse_mode, "text_preview": text[:120]},
        )
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        logger.info(
            "bot_send_message_succeeded",
            "Telegram message sent.",
            {"chat_id": chat_id, "parse_mode": parse_mode},
        )
    except Exception:
        # Иногда ломается Markdown из-за динамических значений (тикеры с _ и т.п.)
        logger.exception(
            "bot_send_message_markdown_failed",
            "Telegram message send with parse_mode failed; retrying without parse mode.",
            {"chat_id": chat_id, "parse_mode": parse_mode},
        )
        await bot.send_message(chat_id=chat_id, text=text)
        logger.info(
            "bot_send_message_plain_succeeded",
            "Telegram message sent without parse mode fallback.",
            {"chat_id": chat_id},
        )


# =============== HELPERS ==================


def classify_operation_group(operation_type: str | None) -> str:
    op_type = (operation_type or "").strip()
    if op_type in DEPOSIT_OPERATION_TYPES:
        return "deposit"
    if op_type in WITHDRAWAL_OPERATION_TYPES:
        return "withdrawal"
    if op_type in BUY_OPERATION_TYPES:
        return "buy"
    if op_type in SELL_OPERATION_TYPES:
        return "sell"
    if op_type in COMMISSION_OPERATION_TYPES:
        return "commission"
    if op_type in INCOME_TAX_OPERATION_TYPES:
        return "income_tax"
    if op_type == "OPERATION_TYPE_DIVIDEND":
        return "dividend"
    if op_type == "OPERATION_TYPE_COUPON":
        return "coupon"
    return "other"


def is_income_event_backed_tax_operation(operation_type: str | None) -> bool:
    op_type = (operation_type or "").strip()
    return op_type in INCOME_EVENT_TAX_OPERATION_TYPES


def to_local_market_date(dt: datetime | None) -> date | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ).date()


def to_iso_datetime(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def decimal_to_str(value: Decimal | float | int | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return format(value, "f")


def json_default(value):
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_csv_file(path: str, fieldnames: list[str], rows: list[dict]):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: (
                        decimal_to_str(value)
                        if isinstance(value, Decimal)
                        else value.isoformat()
                        if isinstance(value, (datetime, date))
                        else value
                    )
                    for key, value in row.items()
                }
            )


def normalize_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def build_logical_asset_id(
    *,
    asset_uid: str | None,
    instrument_uid: str | None,
    figi: str | None,
) -> str | None:
    return asset_uid or instrument_uid or figi


def is_authorized(update: Update) -> bool:
    user = update.effective_user
    if not user:
        logger.warning(
            "bot_update_missing_user",
            "Received update without effective_user.",
            {"update_id": getattr(update, "update_id", None)},
        )
        return False
    if user.id not in ALLOWED_USER_IDS:
        logger.warning(
            "bot_update_unauthorized",
            "Ignored update from unauthorized user.",
            {
                "update_id": getattr(update, "update_id", None),
                "user_id": user.id,
                "username": user.username,
                "chat_id": getattr(update.effective_chat, "id", None),
                "message_text": getattr(update.effective_message, "text", None),
            },
        )
        return False
    return True


def log_update_received(update: Update, command_name: str | None = None) -> None:
    global LAST_UPDATE_RECEIVED_AT_UTC
    LAST_UPDATE_RECEIVED_AT_UTC = datetime.now(timezone.utc)
    logger.info(
        "bot_update_received",
        "Received Telegram update.",
        {
            "update_id": getattr(update, "update_id", None),
            "user_id": getattr(update.effective_user, "id", None),
            "username": getattr(update.effective_user, "username", None),
            "chat_id": getattr(update.effective_chat, "id", None),
            "command": command_name,
            "message_text": getattr(update.effective_message, "text", None),
        },
    )


async def debug_command_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_name = None
    text = getattr(update.effective_message, "text", None) or ""
    if text.startswith("/"):
        command_name = text.split()[0]
    log_update_received(update, command_name=command_name)


def fmt_rub(x: float | None, precision: int = 0) -> str:
    if x is None:
        return "—"
    fmt = f"{{:,.{precision}f}} ₽"
    return fmt.format(x).replace(",", " ")


def fmt_decimal_rub(x: Decimal | float | int | None, precision: int = 2) -> str:
    if x is None:
        x = Decimal("0")
    return fmt_rub(float(x), precision=precision)


def fmt_pct(x: float | None, precision: int = 2) -> str:
    if x is None:
        return "—"
    fmt = f"{{:+.{precision}f}} %"
    return fmt.format(x)


def fmt_signed_amount(x: float, precision: int = 2) -> str:
    value = f"{x:,.{precision}f}".replace(",", " ")
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return f"+{value}"


def fmt_plain_pct(x: float, precision: int = 2) -> str:
    value = f"{x:.{precision}f}"
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return value


def fmt_compact_rub(x: float | None, precision: int = 1, signed: bool = False) -> str:
    if x is None:
        return "—"

    abs_value = abs(x)
    if abs_value >= 1_000_000:
        scaled = x / 1_000_000
        unit = "млн ₽"
    elif abs_value >= 1_000:
        scaled = x / 1_000
        unit = "тыс ₽"
    else:
        fmt = f"{{:+,.0f}}" if signed else "{:,.0f}"
        return f"{fmt.format(x).replace(',', ' ')} ₽"

    fmt = f"{{:+,.{precision}f}}" if signed else f"{{:,.{precision}f}}"
    value = fmt.format(scaled).replace(",", " ")
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return f"{value} {unit}"


def fmt_compact_pct(x: float | None, precision: int = 1, signed: bool = False) -> str:
    if x is None:
        return "—"

    fmt = f"{{:+.{precision}f}}" if signed else f"{{:.{precision}f}}"
    value = fmt.format(x)
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return f"{value} %"


def build_help_text() -> str:
    schedule_label = globals().get("DAILY_JOB_SCHEDULE_LABEL", "18:00")
    return (
        "Доступные команды:\n\n"
        "/today — сводка по портфелю на сегодня\n"
        "/week — сводка по текущей неделе\n"
        "/month — отчёт по текущему месяцу\n"
        "/year [YYYY] — отчёт за год (без аргумента: текущий год YTD)\n"
        "/dataset — архив json+csv+md для AI-анализа\n"
        "/structure — текущая структура портфеля по позициям\n"
        "/history — график стоимости портфеля и суммы пополнений\n"
        "/twr — TWR, XIRR и run-rate на конец года + график по дням\n"
        "/targets — показать текущие таргеты аллокации\n"
        "/targets set stocks=50 bonds=30 cash=20 — задать таргеты по классам\n"
        "/rebalance — показать отклонения и buy/sell для возврата к таргетам\n"
        "/invest <sum> — подсказать, как распределить новое пополнение\n"
        "/help — эта подсказка\n\n"
        "Автоматически:\n"
        f"• каждый день в {schedule_label} — проверка триггеров (максимум, годовой план)\n"
        f"• по пятницам в {schedule_label} — недельный отчёт\n"
        f"• в последний день месяца в {schedule_label} — дополнительный отчёт за месяц\n"
        "• каждое новое пополнение счёта — подсказка, как распределить пополнение по таргетам."
    )


REPORTING_ACCOUNT_UNAVAILABLE_TEXT = (
    "Не удалось определить активный счёт для отчёта. "
    "Укажите корректный TINKOFF_ACCOUNT_ID или дождитесь первого снапшота."
)


def normalize_reporting_account_id(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value or value.lower() == "auto":
        return None
    return value


def choose_reporting_account_id(
    explicit_account_id: str | None,
    latest_snapshot_account_id: str | None,
) -> str | None:
    normalized_explicit = normalize_reporting_account_id(explicit_account_id)
    if normalized_explicit:
        return normalized_explicit

    latest_value = (latest_snapshot_account_id or "").strip()
    return latest_value or None


def compute_twr_series(
    snapshot_rows: list[dict],
    net_external_flow_by_day: dict[date, float],
) -> tuple[list[date], list[float | None], list[float]] | None:
    if len(snapshot_rows) < 2:
        return None

    dates: list[date] = []
    values: list[float | None] = []
    for row in snapshot_rows:
        dates.append(row["snapshot_date"])
        total_value = row.get("total_value")
        values.append(float(total_value) if total_value is not None else None)

    cumulative_multiplier = 1.0
    twr: list[float] = [0.0]
    for idx in range(1, len(dates)):
        previous_value = values[idx - 1]
        current_value = values[idx]
        net_external_flow = net_external_flow_by_day.get(dates[idx], 0.0)

        if previous_value in (None, 0) or current_value is None:
            twr.append(cumulative_multiplier - 1.0)
            continue

        period_return = (current_value - net_external_flow) / previous_value - 1.0
        cumulative_multiplier *= 1.0 + period_return
        twr.append(cumulative_multiplier - 1.0)

    return dates, values, twr


def compute_xnpv(rate: float, cashflows: list[tuple[datetime, float]]) -> float:
    base_dt = cashflows[0][0]
    total = 0.0
    for dt, amount in cashflows:
        years = (dt - base_dt).total_seconds() / (365.0 * 24 * 3600)
        total += amount / ((1.0 + rate) ** years)
    return total


def compute_xirr(cashflows: list[tuple[datetime, float]]) -> float | None:
    if not cashflows:
        return None
    if not any(amount < 0 for _, amount in cashflows):
        return None
    if not any(amount > 0 for _, amount in cashflows):
        return None

    low = -0.9999
    high = 10.0
    low_value = compute_xnpv(low, cashflows)
    high_value = compute_xnpv(high, cashflows)

    expansions = 0
    while low_value * high_value > 0 and expansions < 20:
        high *= 2
        high_value = compute_xnpv(high, cashflows)
        expansions += 1

    if low_value * high_value > 0:
        return None

    for _ in range(200):
        mid = (low + high) / 2.0
        mid_value = compute_xnpv(mid, cashflows)
        if abs(mid_value) < 1e-8:
            return mid
        if low_value * mid_value <= 0:
            high = mid
            high_value = mid_value
        else:
            low = mid
            low_value = mid_value

    return (low + high) / 2.0


def project_run_rate_value(
    current_value: float | None,
    annual_rate: float | None,
    from_date: date,
    to_date: date,
) -> float | None:
    if current_value is None or annual_rate is None:
        return None

    day_count = (to_date - from_date).days
    if day_count < 0:
        return None
    if day_count == 0:
        return current_value

    return current_value * ((1.0 + annual_rate) ** (day_count / 365.0))


def render_twr_summary_text(
    *,
    last_date: date,
    last_value: float | None,
    last_twr_pct: float,
    xirr_value: float | None,
    projected_value: float | None,
    projection_date: date | None,
) -> str:
    calc_date_text = last_date.strftime("%d.%m.%Y")
    projection_date_text = projection_date.strftime("%d.%m.%Y") if projection_date is not None else "—"
    xirr_text = fmt_pct(xirr_value * 100.0, precision=2) if xirr_value is not None else "—"
    projection_text = fmt_rub(projected_value) if projected_value is not None else "—"

    return (
        "📈 *TWR и run-rate*\n"
        f"Дата расчёта: {calc_date_text}\n\n"
        f"*TWR периода*: {fmt_pct(last_twr_pct, precision=2)}\n"
        f"*Текущая стоимость*: {fmt_rub(last_value)}\n\n"
        f"*XIRR*: {xirr_text} годовых\n"
        f"*Run-rate на {projection_date_text}*: {projection_text}\n"
        "_Сценарий: без новых пополнений и выводов._"
    )


def format_month_short_label(d: date) -> str:
    return SHORT_MONTHS_RU[d.month]


def format_day_month_label(d: date, include_year: bool = False) -> str:
    label = f"{d.day} {format_month_short_label(d)}"
    if include_year:
        return f"{label}\n{d.year}"
    return label


def build_month_tick_labels(months: list[date]) -> list[str]:
    if not months:
        return []

    multi_year = len({month.year for month in months}) > 1
    labels: list[str] = []
    prev_year: int | None = None

    for month in months:
        label = format_month_short_label(month)
        if multi_year and month.year != prev_year:
            label = f"{label}\n{month.year}"
        labels.append(label)
        prev_year = month.year

    return labels


def pick_tick_indices(length: int, max_ticks: int = 7) -> list[int]:
    if length <= 0:
        return []
    if length <= max_ticks:
        return list(range(length))

    indices: list[int] = []
    for step in range(max_ticks):
        idx = round(step * (length - 1) / (max_ticks - 1))
        if idx not in indices:
            indices.append(idx)
    return indices


def build_date_ticks(dates: list[date], max_ticks: int = 7) -> tuple[list[date], list[str]]:
    indices = pick_tick_indices(len(dates), max_ticks=max_ticks)
    selected_dates = [dates[idx] for idx in indices]

    labels: list[str] = []
    prev_year: int | None = None
    for dt in selected_dates:
        include_year = prev_year is None or dt.year != prev_year
        labels.append(format_day_month_label(dt, include_year=include_year))
        prev_year = dt.year

    return selected_dates, labels


def rub_axis_formatter(value: float, _pos: int | None = None) -> str:
    return fmt_compact_rub(float(value), precision=1)


def pct_axis_formatter(value: float, _pos: int | None = None) -> str:
    return fmt_compact_pct(float(value), precision=0)


def set_chart_header(fig, title: str, subtitle: str | None = None):
    fig.patch.set_facecolor("white")
    fig.suptitle(
        title,
        x=0.125,
        y=0.972,
        ha="left",
        fontsize=14,
        fontweight="bold",
        color=CHART_COLORS["text"],
    )
    if subtitle:
        fig.text(
            0.125,
            0.905,
            subtitle,
            ha="left",
            va="top",
            fontsize=9,
            color=CHART_COLORS["muted"],
        )


def apply_chart_style(ax, y_formatter=None):
    ax.set_facecolor("white")
    ax.grid(axis="y", color=CHART_COLORS["grid"], linewidth=0.8, alpha=0.7)
    ax.grid(axis="x", visible=False)
    ax.tick_params(axis="both", labelsize=9, colors=CHART_COLORS["muted"])
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))

    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(CHART_COLORS["spine"])
    ax.spines["bottom"].set_color(CHART_COLORS["spine"])

    if y_formatter is not None:
        ax.yaxis.set_major_formatter(FuncFormatter(y_formatter))


def annotate_series_last_point(
    ax,
    x_values: list,
    y_values: list[float],
    label: str,
    color: str,
    y_offset: int = 0,
):
    if not x_values or not y_values:
        return

    last_x = x_values[-1]
    last_y = y_values[-1]
    ax.scatter([last_x], [last_y], color=color, s=28, zorder=5)
    ax.annotate(
        label,
        xy=(last_x, last_y),
        xytext=(10, y_offset),
        textcoords="offset points",
        ha="left",
        va="center",
        fontsize=9,
        color=CHART_COLORS["text"],
        bbox={
            "boxstyle": "round,pad=0.3",
            "fc": "white",
            "ec": color,
            "lw": 1,
            "alpha": 0.96,
        },
        clip_on=False,
        zorder=6,
    )


def annotate_bar_values(ax, x_values: list[int], values: list[float], formatter, text_color: str | None = None):
    visible_values = [abs(value) for value in values if value is not None]
    if not visible_values:
        return

    offset = max(max(visible_values) * 0.04, 1.0)
    for x, value in zip(x_values, values):
        if value is None:
            continue

        label_y = value + offset if value >= 0 else value - offset
        va = "bottom" if value >= 0 else "top"
        color = text_color
        if color is None:
            if value > 0:
                color = CHART_COLORS["positive"]
            elif value < 0:
                color = CHART_COLORS["negative"]
            else:
                color = CHART_COLORS["neutral"]

        ax.text(
            x,
            label_y,
            formatter(value),
            ha="center",
            va=va,
            fontsize=8,
            color=color,
        )


def set_value_axis_limits(
    ax,
    values: list[float],
    min_padding_ratio: float = 0.12,
    flat_padding_ratio: float = 0.05,
):
    if not values:
        return

    min_value = min(values)
    max_value = max(values)
    span = max_value - min_value

    if span <= 0:
        pad = max(abs(max_value) * flat_padding_ratio, 1.0)
        ax.set_ylim(min_value - pad, max_value + pad)
        return

    pad = max(span * min_padding_ratio, 1.0)
    ax.set_ylim(min_value - pad, max_value + pad)


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    first_next = date(d.year, d.month + 1, 1)
    return first_next - timedelta(days=1)


def get_year_period(year: int | None) -> tuple[datetime, datetime, str, bool]:
    today = datetime.now(TZ).date()
    is_ytd = year is None
    period_year = today.year if is_ytd else int(year)

    from_dt = datetime(period_year, 1, 1)
    if is_ytd:
        to_date_inclusive = today
        label = f"{period_year} YTD"
    else:
        to_date_inclusive = date(period_year, 12, 31)
        label = str(period_year)

    to_dt = datetime.combine(to_date_inclusive + timedelta(days=1), time.min)
    return from_dt, to_dt, label, is_ytd


# ============ DB QUERIES (CORE) ===========


def get_latest_snapshot_account_id(session) -> str | None:
    return session.execute(
        text(
            """
            SELECT account_id
            FROM portfolio_snapshots
            ORDER BY snapshot_date DESC, snapshot_at DESC, id DESC
            LIMIT 1
            """
        )
    ).scalar()


def resolve_reporting_account_id(session) -> str | None:
    return choose_reporting_account_id(
        TINKOFF_ACCOUNT_ID,
        get_latest_snapshot_account_id(session),
    )


def get_latest_snapshots(session, account_id: str, limit: int = 2):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT :limit
        """
            ),
            {"account_id": account_id, "limit": limit},
        )
        .mappings()
        .all()
    )
    return list(rows)


def get_latest_snapshot_date(session, account_id: str):
    return session.execute(
        text("SELECT MAX(snapshot_date) FROM portfolio_snapshots WHERE account_id = :account_id"),
        {"account_id": account_id},
    ).scalar_one()


def get_latest_deposit_date(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    return session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT MAX(date::date)
            FROM operations_dedup
            WHERE account_id = :account_id
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()


def get_total_deposits(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def get_deposits_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT COALESCE(SUM(amount), 0) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND date >= :start_dt
          AND date < :end_dt
          AND operation_type IN :operation_types
          AND state = :executed_state
        """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def get_net_external_flow_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
) -> float:
    row = session.execute(
        text(
            f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT COALESCE(
            SUM(
                CASE
                    WHEN operation_type IN :deposit_types THEN ABS(amount)
                    WHEN operation_type IN :withdrawal_types THEN -ABS(amount)
                    ELSE 0
                END
            ),
            0
        ) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND date >= :start_dt
          AND date < :end_dt
          AND operation_type IN :operation_types
          AND state = :executed_state
        """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
            bindparam("withdrawal_types", expanding=True),
            bindparam("operation_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "withdrawal_types": WITHDRAWAL_OPERATION_TYPES,
            "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def _is_undefined_table_error(exc: Exception, table_name: str) -> bool:
    if not isinstance(exc, ProgrammingError):
        return False
    if getattr(exc.orig, "pgcode", None) == "42P01":
        return True
    return f'relation "{table_name}" does not exist' in str(exc).lower()


def get_income_for_period(db, account_id: str, start_date, end_date) -> tuple[Decimal, Decimal]:
    try:
        row = db.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
                    COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"account_id": account_id, "start_date": start_date, "end_date": end_date},
        ).mappings().one()
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            return Decimal("0"), Decimal("0")
        raise

    return Decimal(row["coupons"] or 0), Decimal(row["dividends"] or 0)


def get_commissions_for_period(db, account_id: str, start_date, end_date) -> Decimal:
    total = db.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": COMMISSION_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return abs(Decimal(total or 0))


def get_taxes_for_period(db, account_id: str, start_date, end_date) -> Decimal:
    income_taxes = Decimal("0")
    try:
        income_taxes_row = db.execute(
            text(
                """
                SELECT COALESCE(SUM(tax_amount), 0) AS total
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"account_id": account_id, "start_date": start_date, "end_date": end_date},
        ).scalar_one()
        income_taxes = Decimal(income_taxes_row or 0)
    except Exception as exc:
        if not _is_undefined_table_error(exc, "income_events"):
            raise

    operation_taxes = db.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(ABS(amount)), 0) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return income_taxes + Decimal(operation_taxes or 0)


def get_month_snapshots(session, account_id: str, year: int, month: int):
    month_start = date(year, month, 1)
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    # последний снапшот до начала месяца — база для расчёта дельты
    start_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": month_start, "end": next_month_start},
        )
        .mappings()
        .first()
    )

    # последний снапшот в месяце
    end_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": month_start, "end": next_month_start},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_period_snapshots(session, account_id: str, start_date: date, end_date_exclusive: date):
    """
    Возвращает пару снапшотов для периода:
    - start_row: последний снапшот строго до start_date
    - end_row:   последний снапшот внутри [start_date, end_date_exclusive)
    """
    start_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": start_date},
        )
        .mappings()
        .first()
    )

    end_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": start_date, "end": end_date_exclusive},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_latest_snapshot_with_id(session, account_id: str):
    row = (
        session.execute(
            text(
                """
        SELECT
            id,
            account_id,
            snapshot_date,
            snapshot_at,
            total_value,
            currency,
            total_shares,
            total_bonds,
            total_etf,
            total_currencies,
            total_futures
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .first()
    )
    return row


def get_dataset_bounds(session, account_id: str):
    row = (
        session.execute(
            text(
                """
                SELECT
                    MIN(snapshot_date) AS min_date,
                    MAX(snapshot_date) AS max_date
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .one()
    )
    return row


def get_daily_snapshot_rows(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
                SELECT
                    id,
                    snapshot_date,
                    snapshot_at,
                    currency,
                    total_value,
                    expected_yield,
                    expected_yield_pct
                FROM (
                    SELECT
                        id,
                        snapshot_date,
                        snapshot_at,
                        currency,
                        total_value,
                        expected_yield,
                        expected_yield_pct,
                        ROW_NUMBER() OVER (
                            PARTITION BY snapshot_date
                            ORDER BY snapshot_at DESC, id DESC
                        ) AS rn
                    FROM portfolio_snapshots
                    WHERE account_id = :account_id
                ) daily
                WHERE rn = 1
                ORDER BY snapshot_date ASC
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    return rows


def get_positions_for_snapshot(session, snapshot_id: int):
    query = """
        SELECT
            figi,
            COALESCE(ticker, '') AS ticker,
            COALESCE(name, '')   AS name,
            instrument_uid,
            position_uid,
            asset_uid,
            instrument_type,
            quantity,
            currency,
            current_price,
            current_nkd,
            position_value,
            expected_yield,
            expected_yield_pct,
            weight_pct
        FROM portfolio_positions
        WHERE snapshot_id = :sid
        ORDER BY position_value DESC
    """
    fallback_query = """
        SELECT
            figi,
            COALESCE(ticker, '') AS ticker,
            COALESCE(name, '')   AS name,
            NULL AS instrument_uid,
            NULL AS position_uid,
            NULL AS asset_uid,
            instrument_type,
            quantity,
            currency,
            current_price,
            NULL AS current_nkd,
            position_value,
            expected_yield,
            expected_yield_pct,
            weight_pct
        FROM portfolio_positions
        WHERE snapshot_id = :sid
        ORDER BY position_value DESC
    """
    try:
        rows = (
            session.execute(text(query), {"sid": snapshot_id})
            .mappings()
            .all()
        )
    except Exception as exc:
        if not _is_undefined_table_error(exc, "portfolio_positions") and "column" not in str(exc).lower():
            raise
        session.rollback()
        rows = (
            session.execute(text(fallback_query), {"sid": snapshot_id})
            .mappings()
            .all()
        )
    return rows


def get_dataset_operations(session, account_id: str, start_dt: datetime, end_dt: datetime):
    rows = (
        session.execute(
            text(
                """
                WITH operations_dedup AS (
                    SELECT DISTINCT ON (account_id, COALESCE(operation_id, id::text))
                        account_id,
                        operation_id,
                        date,
                        amount,
                        currency,
                        operation_type,
                        state,
                        instrument_uid,
                        asset_uid,
                        figi,
                        name,
                        commission,
                        yield,
                        description,
                        source,
                        price,
                        quantity
                    FROM operations
                    ORDER BY account_id, COALESCE(operation_id, id::text), id DESC
                )
                SELECT
                    operation_id,
                    date,
                    amount,
                    currency,
                    operation_type,
                    state,
                    instrument_uid,
                    asset_uid,
                    figi,
                    name,
                    commission,
                    yield,
                    description,
                    source,
                    price,
                    quantity
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :start_dt
                  AND date < :end_dt
                  AND state = :executed_state
                ORDER BY date ASC, operation_id ASC NULLS LAST
                """
            ),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_asset_alias_rows(session):
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT
                        asset_uid,
                        instrument_uid,
                        figi,
                        ticker,
                        name,
                        first_seen_at,
                        last_seen_at
                    FROM asset_aliases
                    ORDER BY asset_uid ASC, last_seen_at DESC
                    """
                )
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "asset_aliases"):
            session.rollback()
            return []
        raise
    return rows


def get_income_events_for_period(session, account_id: str, start_date: date, end_date: date):
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT
                        ie.event_date,
                        ie.event_type,
                        ie.figi,
                        COALESCE(i.ticker, '') AS ticker,
                        COALESCE(i.name, ie.figi) AS instrument_name,
                        ie.gross_amount,
                        ie.tax_amount,
                        ie.net_amount,
                        ie.net_yield_pct,
                        ie.notified
                    FROM income_events ie
                    LEFT JOIN instruments i ON i.figi = ie.figi
                    WHERE ie.account_id = :account_id
                      AND ie.event_date >= :start_date
                      AND ie.event_date <= :end_date
                    ORDER BY ie.event_date ASC, ie.figi ASC, ie.event_type ASC
                    """
                ),
                {"account_id": account_id, "start_date": start_date, "end_date": end_date},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            session.rollback()
            return []
        raise
    return rows


def build_asset_alias_lookup(alias_rows: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_instrument_uid: dict[str, dict] = {}
    by_figi: dict[str, dict] = {}
    for row in alias_rows:
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid and instrument_uid not in by_instrument_uid:
            by_instrument_uid[instrument_uid] = row
        if figi and figi not in by_figi:
            by_figi[figi] = row
    return by_instrument_uid, by_figi


def build_reconciliation_by_asset_type(
    latest_snapshot: dict,
    positions_rows: list[dict],
) -> tuple[list[dict], Decimal, Decimal]:
    positions_sum_total = Decimal("0")
    grouped_position_sums: dict[str, Decimal] = {}
    grouped_current_nkd_sums: dict[str, Decimal] = {}

    for row in positions_rows:
        instrument_type = (row.get("instrument_type") or "other").strip().lower()
        position_value = normalize_decimal(row.get("position_value"))
        current_nkd = normalize_decimal(row.get("current_nkd"))
        positions_sum_total += position_value
        grouped_position_sums[instrument_type] = grouped_position_sums.get(instrument_type, Decimal("0")) + position_value
        grouped_current_nkd_sums[instrument_type] = grouped_current_nkd_sums.get(instrument_type, Decimal("0")) + current_nkd

    reconciliation_rows: list[dict] = []
    for instrument_type, snapshot_field in SNAPSHOT_TOTAL_FIELDS.items():
        if instrument_type == "future":
            continue
        snapshot_total = normalize_decimal(latest_snapshot.get(snapshot_field))
        positions_sum = grouped_position_sums.get(instrument_type, Decimal("0"))
        current_nkd_sum = grouped_current_nkd_sums.get(instrument_type, Decimal("0"))
        reconciliation_rows.append(
            {
                "asset_type": instrument_type,
                "snapshot_total": snapshot_total,
                "positions_sum": positions_sum,
                "gap_abs": snapshot_total - positions_sum,
                "observed_current_nkd_sum": current_nkd_sum,
            }
        )

    reconciliation_gap_abs = normalize_decimal(latest_snapshot.get("total_value")) - positions_sum_total
    return reconciliation_rows, positions_sum_total, reconciliation_gap_abs


def compute_positions_diff_lines(start_positions, end_positions) -> list[str]:
    def _qty(value) -> float:
        if value is None:
            return 0.0
        return float(value)

    def _ticker(pos: dict) -> str:
        return (pos.get("ticker") or pos.get("figi") or "UNKNOWN").strip()

    start_map = {
        str(pos.get("figi")): pos
        for pos in start_positions
        if pos.get("figi") is not None
    }
    end_map = {
        str(pos.get("figi")): pos
        for pos in end_positions
        if pos.get("figi") is not None
    }

    new_items: list[tuple[str, str]] = []
    closed_items: list[tuple[str, str]] = []
    up_items: list[tuple[str, str]] = []
    down_items: list[tuple[str, str]] = []

    all_figis = sorted(set(start_map.keys()) | set(end_map.keys()))
    for figi in all_figis:
        start_pos = start_map.get(figi)
        end_pos = end_map.get(figi)

        if start_pos is None and end_pos is not None:
            end_qty = _qty(end_pos.get("quantity"))
            ticker = _ticker(end_pos)
            new_items.append((ticker, f"+ {ticker} — {end_qty:.0f} шт (новая)"))
            continue

        if start_pos is not None and end_pos is None:
            start_qty = _qty(start_pos.get("quantity"))
            ticker = _ticker(start_pos)
            closed_items.append((ticker, f"- {ticker} — {start_qty:.0f} шт (закрыта)"))
            continue

        start_qty = _qty(start_pos.get("quantity"))
        end_qty = _qty(end_pos.get("quantity"))
        qty_diff = end_qty - start_qty
        if qty_diff > 0:
            ticker = _ticker(end_pos)
            up_items.append(
                (ticker, f"↑ {ticker} — +{qty_diff:.0f} шт ({start_qty:.0f} → {end_qty:.0f})")
            )
        elif qty_diff < 0:
            ticker = _ticker(end_pos)
            down_items.append(
                (ticker, f"↓ {ticker} — -{abs(qty_diff):.0f} шт ({start_qty:.0f} → {end_qty:.0f})")
            )

    new_lines = [line for _, line in sorted(new_items, key=lambda item: item[0])]
    closed_lines = [line for _, line in sorted(closed_items, key=lambda item: item[0])]
    up_lines = [line for _, line in sorted(up_items, key=lambda item: item[0])]
    down_lines = [line for _, line in sorted(down_items, key=lambda item: item[0])]

    return new_lines + closed_lines + up_lines + down_lines


def compute_positions_diff_grouped(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[list[str], str | None]:
    def _qty(value) -> float:
        if value is None:
            return 0.0
        return float(value)

    def _fmt_qty(value: float) -> str:
        if value.is_integer():
            return f"{int(value)}"
        return f"{value:.6f}".rstrip("0").rstrip(".")

    def _asset_key(pos: dict) -> str:
        ticker = (pos.get("instrument_ticker") or pos.get("position_ticker") or "").strip()
        name = (pos.get("instrument_name") or pos.get("position_name") or "").strip()
        figi = (pos.get("figi") or "UNKNOWN").strip()
        if ticker:
            return ticker
        if name:
            return f"NAME:{name}"
        return f"FIGI:{figi}"

    def _display_name(pos: dict) -> str:
        ticker = (pos.get("instrument_ticker") or pos.get("position_ticker") or "").strip()
        name = (pos.get("instrument_name") or pos.get("position_name") or "").strip()
        figi = (pos.get("figi") or "UNKNOWN").strip()
        if name and ticker:
            return f"{name} ({ticker})"
        if name:
            return name
        if ticker:
            return ticker
        return figi

    snapshot_bounds = session.execute(
        text(
            """
            SELECT id, snapshot_date, snapshot_at
            FROM portfolio_snapshots
            WHERE account_id = :account_id
              AND snapshot_at >= :from_dt
              AND snapshot_at < :to_dt
            ORDER BY snapshot_date ASC, snapshot_at ASC
            """
        ),
        {"account_id": account_id, "from_dt": from_dt, "to_dt": to_dt},
    ).mappings().all()

    if len(snapshot_bounds) < 2:
        return [], "За выбранный период недостаточно снапшотов для сравнения позиций."

    start_snapshot = snapshot_bounds[0]
    end_snapshot = snapshot_bounds[-1]
    start_snapshot_id = start_snapshot["id"]
    end_snapshot_id = end_snapshot["id"]
    show_new_block = start_snapshot["snapshot_date"] == date(from_dt.year, 1, 1)

    rows = session.execute(
        text(
            """
            SELECT
                pp.snapshot_id,
                pp.figi,
                pp.quantity,
                pp.ticker AS position_ticker,
                pp.name AS position_name,
                pp.instrument_type AS position_instrument_type,
                i.ticker AS instrument_ticker,
                i.name AS instrument_name,
                i.instrument_type AS instrument_type
            FROM portfolio_positions pp
            LEFT JOIN instruments i ON i.figi = pp.figi
            WHERE pp.snapshot_id IN (:start_snapshot_id, :end_snapshot_id)
            """
        ),
        {"start_snapshot_id": start_snapshot_id, "end_snapshot_id": end_snapshot_id},
    ).mappings().all()

    start_qty_by_key: dict[str, float] = {}
    end_qty_by_key: dict[str, float] = {}
    start_figis_by_key: dict[str, set[str]] = {}
    end_figis_by_key: dict[str, set[str]] = {}
    display_by_key: dict[str, str] = {}

    for row in rows:
        figi = str(row.get("figi") or "").strip()
        if not figi:
            continue

        instrument_type = (row.get("instrument_type") or row.get("position_instrument_type") or "")
        if figi == "RUB000UTSTOM" or str(instrument_type).lower() == "currency":
            continue

        key = _asset_key(row)
        display_by_key.setdefault(key, _display_name(row))
        qty = _qty(row.get("quantity"))

        if row["snapshot_id"] == start_snapshot_id:
            start_qty_by_key[key] = start_qty_by_key.get(key, 0.0) + qty
            start_figis_by_key.setdefault(key, set()).add(figi)
        elif row["snapshot_id"] == end_snapshot_id:
            end_qty_by_key[key] = end_qty_by_key.get(key, 0.0) + qty
            end_figis_by_key.setdefault(key, set()).add(figi)

    grouped: list[tuple[str, str, str]] = []
    all_keys = sorted(set(start_qty_by_key.keys()) | set(end_qty_by_key.keys()))
    for key in all_keys:
        qty0 = start_qty_by_key.get(key, 0.0)
        qty1 = end_qty_by_key.get(key, 0.0)
        name = display_by_key.get(key, key)

        if qty0 == 0 and qty1 > 0:
            grouped.append(("🆕 Новые", name, f"+ {name}: {_fmt_qty(0.0)} → {_fmt_qty(qty1)} шт"))
        elif qty0 > 0 and qty1 == 0:
            grouped.append(("✅ Закрыли", name, f"- {name}: {_fmt_qty(qty0)} → {_fmt_qty(0.0)} шт"))
        elif qty1 > qty0:
            grouped.append(("📈 Докупили", name, f"↑ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт"))
        elif qty1 < qty0 and qty1 > 0:
            grouped.append(("📉 Продали часть", name, f"↓ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт"))

    categories = ["📈 Докупили", "📉 Продали часть", "✅ Закрыли"]
    if show_new_block:
        categories = ["🆕 Новые", *categories]
    grouped_lines: list[str] = []
    for category in categories:
        category_items = sorted([item for item in grouped if item[0] == category], key=lambda item: item[1])
        if not category_items:
            continue
        if grouped_lines:
            grouped_lines.append("")
        grouped_lines.append(category)
        grouped_lines.extend(line for _, _, line in category_items)

    return grouped_lines, None


def get_portfolio_timeseries(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date ASC
        """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    return rows


def get_deposits_by_date(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    rows = (
        session.execute(
            text(
                f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT date::date AS d, SUM(amount) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND state = :executed_state
        GROUP BY date::date
        ORDER BY d ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": operation_types,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_year_financials_from_operations(session, account_id: str, start_dt: datetime, end_dt: datetime) -> dict[str, Decimal]:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT
                COALESCE(SUM(CASE WHEN operation_type IN :deposit_types THEN amount ELSE 0 END), 0) AS deposits,
                COALESCE(SUM(CASE
                    WHEN operation_type IN ('OPERATION_TYPE_DIVIDEND', 'OPERATION_TYPE_DIVIDEND_TAX') THEN amount
                    ELSE 0
                END), 0) AS dividend_net,
                COALESCE(SUM(CASE
                    WHEN operation_type IN ('OPERATION_TYPE_COUPON', 'OPERATION_TYPE_COUPON_TAX') THEN amount
                    ELSE 0
                END), 0) AS coupon_net
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
            """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).mappings().one()

    dividend_net = Decimal(row["dividend_net"] or 0)
    coupon_net = Decimal(row["coupon_net"] or 0)
    income_net = dividend_net + coupon_net

    return {
        "deposits": Decimal(row["deposits"] or 0),
        "income_net": income_net,
        "dividend_net": dividend_net,
        "coupon_net": coupon_net,
    }


def compute_realized_by_asset(session, account_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    od.figi,
                    COALESCE(NULLIF(MAX(NULLIF(od.name, '')), ''), NULLIF(MAX(NULLIF(i.name, '')), ''), od.figi) AS name,
                    COALESCE(NULLIF(MAX(NULLIF(i.ticker, '')), ''), '') AS ticker,
                    COALESCE(SUM(COALESCE(od.yield, 0) + COALESCE(od.commission, 0)), 0) AS amount
                FROM operations_dedup od
                LEFT JOIN instruments i ON i.figi = od.figi
                WHERE od.account_id = :account_id
                  AND od.date >= :start_dt
                  AND od.date < :end_dt
                  AND od.state = :executed_state
                  AND od.operation_type = 'OPERATION_TYPE_SELL'
                  AND od.figi IS NOT NULL
                GROUP BY od.figi
                ORDER BY amount DESC
                """
            ),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )

    parsed = []
    total = Decimal('0')
    for row in rows:
        amount = Decimal(row['amount'] or 0)
        total += amount
        parsed.append(
            {
                'figi': row['figi'],
                'name': row['name'] or row['figi'],
                'ticker': row['ticker'] or '',
                'amount': amount,
            }
        )
    return parsed, total


def compute_income_by_asset_net(session, account_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    od.figi,
                    COALESCE(NULLIF(MAX(NULLIF(od.name, '')), ''), NULLIF(MAX(NULLIF(i.name, '')), ''), od.figi) AS name,
                    COALESCE(NULLIF(MAX(NULLIF(i.ticker, '')), ''), '') AS ticker,
                    COALESCE(SUM(
                        CASE
                            WHEN od.operation_type IN ('OPERATION_TYPE_DIVIDEND', 'OPERATION_TYPE_COUPON') THEN od.amount
                            WHEN od.operation_type IN ('OPERATION_TYPE_DIVIDEND_TAX', 'OPERATION_TYPE_COUPON_TAX') THEN od.amount
                            ELSE 0
                        END
                    ), 0) AS net_amount
                FROM operations_dedup od
                LEFT JOIN instruments i ON i.figi = od.figi
                WHERE od.account_id = :account_id
                  AND od.date >= :start_dt
                  AND od.date < :end_dt
                  AND od.state = :executed_state
                  AND od.operation_type IN (
                      'OPERATION_TYPE_DIVIDEND',
                      'OPERATION_TYPE_DIVIDEND_TAX',
                      'OPERATION_TYPE_COUPON',
                      'OPERATION_TYPE_COUPON_TAX'
                  )
                  AND od.figi IS NOT NULL
                GROUP BY od.figi
                ORDER BY net_amount DESC
                """
            ),
            {
                'account_id': account_id,
                'start_dt': start_dt,
                'end_dt': end_dt,
                'executed_state': EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )

    parsed = []
    total = Decimal('0')
    for row in rows:
        amount = Decimal(row['net_amount'] or 0)
        total += amount
        parsed.append(
            {
                'figi': row['figi'],
                'name': row['name'] or row['figi'],
                'ticker': row['ticker'] or '',
                'amount': amount,
            }
        )
    return parsed, total


def get_unrealized_at_period_end(session, account_id: str, to_dt: datetime) -> Decimal:
    to_date = to_dt.date() - timedelta(days=1)
    snap = (
        session.execute(
            text(
                """
                SELECT id, expected_yield
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date <= :to_date
                ORDER BY snapshot_date DESC, snapshot_at DESC
                LIMIT 1
                """
            ),
            {'account_id': account_id, 'to_date': to_date},
        )
        .mappings()
        .first()
    )
    if not snap:
        return Decimal('0')

    positions_sum = session.execute(
        text(
            """
            SELECT SUM(expected_yield)
            FROM portfolio_positions
            WHERE snapshot_id = :sid
            """
        ),
        {'sid': snap['id']},
    ).scalar_one()

    if positions_sum is not None:
        return Decimal(positions_sum)

    snapshot_yield = snap.get('expected_yield')
    if snapshot_yield is None:
        return Decimal('0')
    return Decimal(snapshot_yield)


def get_year_deposits_by_date(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
):
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT date::date AS d, SUM(amount) AS s
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :start_dt
                  AND date < :end_dt
                  AND operation_type IN :operation_types
                  AND state = :executed_state
                GROUP BY date::date
                ORDER BY d ASC
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "operation_types": DEPOSIT_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_portfolio_values(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
    is_ytd: bool,
):
    rows = (
        session.execute(
            text(
                """
                SELECT month_start, total_value
                FROM (
                    SELECT
                        date_trunc('month', snapshot_date)::date AS month_start,
                        total_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('month', snapshot_date)
                            ORDER BY snapshot_date DESC, snapshot_at DESC
                        ) AS rn
                    FROM portfolio_snapshots
                    WHERE account_id = :account_id
                      AND snapshot_date >= :from_date
                      AND snapshot_date < :to_date
                ) month_snaps
                WHERE rn = 1
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_date": from_dt.date(),
                "to_date": to_dt.date(),
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_deposits(session, account_id: str, from_dt: datetime, to_dt: datetime):
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    date_trunc('month', date)::date AS month_start,
                    SUM(amount) AS amount
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type = 'OPERATION_TYPE_INPUT'
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_last_snapshot_before_date(session, account_id: str, d: date):
    return (
        session.execute(
            text(
                """
                SELECT snapshot_date, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date < :d
                ORDER BY snapshot_date DESC, snapshot_at DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "d": d},
        )
        .mappings()
        .first()
    )


def get_first_snapshot_in_period(session, account_id: str, from_date: date, to_date: date):
    return (
        session.execute(
            text(
                """
                SELECT snapshot_date, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date >= :from_date
                  AND snapshot_date < :to_date
                ORDER BY snapshot_date ASC, snapshot_at ASC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "from_date": from_date, "to_date": to_date},
        )
        .mappings()
        .first()
    )


def get_deposits_sum_for_period(session, account_id: str, start_dt: datetime, end_dt: datetime) -> float:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
              AND operation_type = 'OPERATION_TYPE_INPUT'
            """
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)

def get_portfolio_timeseries_agg_by_date(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, total_value
        FROM (
            SELECT
                snapshot_date,
                total_value,
                ROW_NUMBER() OVER (
                    PARTITION BY snapshot_date
                    ORDER BY snapshot_at DESC, id DESC
                ) AS rn
            FROM portfolio_snapshots
            WHERE account_id = :account_id
        ) daily
        WHERE rn = 1
        ORDER BY snapshot_date ASC
        """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    return rows


def get_external_cashflows_raw(session, account_id: str):
    rows = (
        session.execute(
            text(
                f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT date, amount, operation_type
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND state = :executed_state
        ORDER BY date ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_max_value_before_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :d
        """
        ),
        {"account_id": account_id, "d": d},
    ).scalar_one()
    return float(row) if row is not None else None


def get_max_value_to_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date <= :d
        """
        ),
        {"account_id": account_id, "d": d},
    ).scalar_one()
    return float(row) if row is not None else None


# ========= BUSINESS CALCULATIONS ==========


def compute_period_delta_excluding_external_flow(
    start_value: float | None,
    end_value: float | None,
    net_external_flow: float,
) -> tuple[float | None, float | None]:
    if start_value in (None, 0) or end_value is None:
        return None, None

    delta_abs = end_value - start_value - net_external_flow
    delta_pct = delta_abs / start_value * 100.0
    return delta_abs, delta_pct


def build_today_summary() -> str:
    """
    Формирует текст сводки "на сегодня" используя шаблоны из today_templates.
    """
    now_local = datetime.now(TZ)
    day_start = datetime.combine(now_local.date(), time.min)
    day_end_exclusive = day_start + timedelta(days=1)
    day_end = day_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        snaps = get_latest_snapshots(session, account_id, limit=2)
        net_external_flow_today = get_net_external_flow_for_period(
            session,
            account_id,
            day_start,
            day_end_exclusive,
        )
        total_deposits = get_total_deposits(session, account_id)
        coupons, dividends = get_income_for_period(session, account_id, day_start, day_end)
        commissions = get_commissions_for_period(session, account_id, day_start, day_end)
        taxes = get_taxes_for_period(session, account_id, day_start, day_end)

    if not snaps:
        return "Пока нет ни одного снапшота портфеля."

    last = snaps[0]
    last_value = float(last["total_value"]) if last["total_value"] is not None else None
    # Конвертируем время снапшота в локальную таймзону
    if last["snapshot_at"]:
        dt = last["snapshot_at"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        snapshot_dt_local = dt.astimezone(TZ)
        snapshot_dt_str = snapshot_dt_local.strftime("%d.%m.%y %H:%M")
    else:
        # Fallback, если snapshot_at вдруг NULL
        snapshot_dt_str = last["snapshot_date"].strftime("%d.%m.%y")

    prev_value = None
    if len(snaps) >= 2:
        prev = snaps[1]
        prev_value = float(prev["total_value"]) if prev["total_value"] is not None else None

    # Изменение к предыдущему дню
    delta_abs = None
    delta_pct = None
    if last_value is not None and prev_value is not None:
        delta_abs, delta_pct = compute_period_delta_excluding_external_flow(
            prev_value,
            last_value,
            net_external_flow_today,
        )

    # Общая доходность
    pnl_abs = None
    pnl_pct = None
    if last_value is not None and total_deposits > 0:
        pnl_abs = last_value - total_deposits
        pnl_pct = pnl_abs / total_deposits * 100.0

    ctx = TodayContext(
        snapshot_dt=snapshot_dt_str,
        current_value=fmt_rub(last_value),
        delta_abs=fmt_rub(delta_abs) if delta_abs is not None else "—",
        delta_pct=fmt_pct(delta_pct) if delta_pct is not None else "—",
        pnl_abs=fmt_rub(pnl_abs) if pnl_abs is not None else "—",
        pnl_pct=fmt_pct(pnl_pct) if pnl_pct is not None else "—",
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return render_today_text(ctx)


def build_week_summary() -> str:
    """
    Формирует текст еженедельной сводки используя шаблоны из week_templates.
    """
    now_local = datetime.now(TZ)
    week_start_date = now_local.date() - timedelta(days=now_local.weekday())
    week_end_date = week_start_date + timedelta(days=4)
    week_start = datetime.combine(week_start_date, time.min)
    week_end_exclusive = datetime.combine(week_end_date + timedelta(days=1), time.min)
    week_end = week_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        # 1. Определяем даты текущей рабочей недели (понедельник–пятница)
        latest_snap = get_latest_snapshot_with_id(session, account_id)
        if not latest_snap:
            return "Пока нет ни одного снапшота портфеля."

        # Формируем week_label (например "10–14 ноября 2025")
        if week_start_date.month == week_end_date.month:
            month_name = MONTHS_RU.get(week_end_date.month, str(week_end_date.month))
            week_label = f"{week_start_date.day}–{week_end_date.day} {month_name} {week_end_date.year}"
        else:
            start_month_name = MONTHS_RU.get(week_start_date.month, str(week_start_date.month))
            end_month_name = MONTHS_RU.get(week_end_date.month, str(week_end_date.month))
            week_label = (
                f"{week_start_date.day} {start_month_name}–"
                f"{week_end_date.day} {end_month_name} {week_end_date.year}"
            )

        # 2. Текущая стоимость
        current_value = float(latest_snap["total_value"]) if latest_snap["total_value"] is not None else 0.0

        # Внешний cashflow за рабочую неделю для корректной дельты
        net_external_flow_week = get_net_external_flow_for_period(
            session,
            account_id,
            week_start,
            week_end_exclusive,
        )

        # 3. Изменение за неделю
        # Ищем снапшот до начала недели, чтобы посчитать дельту
        # Если снапшота ровно в start_date нет, берем ближайший предыдущий
        # Если портфель моложе недели, берем самый первый
        start_row = get_last_snapshot_before_date(session, account_id, week_start_date)
        start_val_row = start_row["total_value"] if start_row is not None else None

        start_value = float(start_val_row) if start_val_row is not None else 0.0

        week_delta_abs = None
        week_delta_pct = None
        
        # Если start_value == 0 или None, значит до начала недели данных не было
        # Но если портфель создан внутри недели, можно считать start_value = 0?
        # Будем считать дельту только если есть старое значение.
        if start_val_row is not None and start_value != 0:
            week_delta_abs, week_delta_pct = compute_period_delta_excluding_external_flow(
                start_value,
                current_value,
                net_external_flow_week,
            )
        elif start_val_row is None:
            # Портфель появился на этой неделе
            week_delta_abs = current_value - net_external_flow_week
            week_delta_pct = 0.0 # Или None, как удобнее

        # 4. Пополнения/доходы/расходы за текущую рабочую неделю
        dep_week = get_deposits_for_period(session, account_id, week_start, week_end_exclusive)
        coupons, dividends = get_income_for_period(session, account_id, week_start, week_end)
        commissions = get_commissions_for_period(session, account_id, week_start, week_end)
        taxes = get_taxes_for_period(session, account_id, week_start, week_end)

        # 5. Прогресс по годовому плану
        year_start = datetime(week_end_date.year, 1, 1)
        dep_year = get_deposits_for_period(session, account_id, year_start, week_end_exclusive)
        
        plan = PLAN_ANNUAL_CONTRIB_RUB
        plan_pct = (dep_year / plan * 100.0) if plan > 0 else 0.0

    ctx = WeekContext(
        week_label=week_label,
        current_value=fmt_rub(current_value),
        week_delta_abs=fmt_rub(week_delta_abs) if week_delta_abs is not None else "—",
        week_delta_pct=fmt_pct(week_delta_pct) if week_delta_pct is not None else "—",
        dep_week=fmt_rub(dep_week),
        plan_progress_pct=f"{plan_pct:.1f} %",
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return render_week_text(ctx)


# Фразы для /month (мягкие, во множественном числе)
PHRASES_AHEAD = [
    "Вы идёте чуть впереди ориентировочного графика — так держать 💪",
    "По взносам вы уже обгоняете план — можно даже немного выдохнуть 😊",
    "График пополнений опережает плановый — отличный темп!",
    "Вы двигаетесь быстрее ориентирного графика — это даёт хороший запас по времени.",
    "План по пополнениям сейчас даже немного обгоняете — очень комфортная позиция.",
]

PHRASES_ON_TRACK = [
    "Вы идёте примерно по ориентировочному графику — всё в порядке 👍",
    "Пополнения близки к графику, можно продолжать в том же духе.",
    "Вы держите нормальный темп, ориентир по году пока соблюдается.",
    "По взносам вы сейчас около плановой траектории — всё идёт по плану.",
    "Глобально вы находитесь рядом с графиком — можно спокойно продолжать.",
]

PHRASES_BEHIND = [
    "Сейчас вы чуть позади ориентировочного графика, но это легко наверстать.",
    "Темп пополнений пока ниже плана, но у вас есть время выровнять траекторию.",
    "Есть небольшой лаг по взносам — можно подумать, как закрыть его в ближайшие месяцы.",
    "Сейчас вы чуть отстаёте от ориентирного графика, но критичного отставания нет.",
    "Темп взносов ниже планового, но с учётом горизонта это ещё поправимо.",
]


def build_month_summary() -> str:
    """
    Отчёт по текущему месяцу используя шаблоны из month_templates.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    year = today.year
    month = today.month

    month_start = date(year, month, 1)
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    month_start_dt = datetime.combine(month_start, time.min)
    month_end_exclusive = datetime.combine(next_month_start, time.min)
    month_end_dt = month_end_exclusive - timedelta(microseconds=1)

    year_start = date(year, 1, 1)
    next_year_start = date(year + 1, 1, 1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        # Пополнения за месяц
        dep_month = get_deposits_for_period(
            session,
            account_id=account_id,
            start_dt=month_start_dt,
            end_dt=month_end_exclusive,
        )
        net_external_flow_month = get_net_external_flow_for_period(
            session,
            account_id=account_id,
            start_dt=month_start_dt,
            end_dt=month_end_exclusive,
        )

        coupons, dividends = get_income_for_period(session, account_id, month_start_dt, month_end_dt)
        commissions = get_commissions_for_period(session, account_id, month_start_dt, month_end_dt)
        taxes = get_taxes_for_period(session, account_id, month_start_dt, month_end_dt)

        # Пополнения за год
        dep_year = get_deposits_for_period(
            session,
            account_id=account_id,
            start_dt=datetime(year, 1, 1),
            end_dt=month_end_exclusive,
        )

        # Снапшоты для изменения стоимости за месяц
        start_snap, end_snap = get_month_snapshots(session, account_id, year, month)

        start_positions = []
        end_positions = []
        if start_snap:
            start_positions = get_positions_for_snapshot(session, start_snap["id"])
        if end_snap:
            end_positions = get_positions_for_snapshot(session, end_snap["id"])

    # План по году
    plan = PLAN_ANNUAL_CONTRIB_RUB
    year_pct = dep_year / plan * 100.0 if plan > 0 else 0.0

    days_in_year = (next_year_start - year_start).days
    days_passed = (today - year_start).days + 1  # включительно
    target_to_date = plan * days_passed / days_in_year if days_in_year > 0 else None

    # Оценка "впереди/по графику/позади"
    status_phrase = ""
    if target_to_date is not None and plan > 0:
        if dep_year >= target_to_date * 1.05:
            status_phrase = random.choice(PHRASES_AHEAD)
        elif dep_year >= target_to_date * 0.95:
            status_phrase = random.choice(PHRASES_ON_TRACK)
        else:
            status_phrase = random.choice(PHRASES_BEHIND)

    # Динамика стоимости портфеля за месяц
    delta_abs = None
    delta_pct = None
    current_value = 0.0
    
    if end_snap:
        current_value = float(end_snap["total_value"])
        
    if start_snap and end_snap:
        start_val = float(start_snap["total_value"])
        end_val = float(end_snap["total_value"])
        delta_abs, delta_pct = compute_period_delta_excluding_external_flow(
            start_val,
            end_val,
            net_external_flow_month,
        )

    # Формирование контекста шаблона
    month_name = MONTHS_RU.get(month, str(month))
    month_year_label = f"{month_name} {year}"

    ctx = MonthContext(
        month_year_label=month_year_label,
        current_value=fmt_rub(current_value),
        dep_month=fmt_rub(dep_month),
        dep_year=fmt_rub(dep_year),
        year_plan=fmt_rub(plan),
        year_progress_pct=f"{year_pct:.1f} %",
        delta_month_abs=fmt_rub(delta_abs) if delta_abs is not None else "—",
        delta_month_pct=fmt_pct(delta_pct, precision=2) if delta_pct is not None else "—",
        plan_status_phrase=status_phrase,
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    month_text = render_month_text(ctx)
    if end_snap:
        diff_lines = compute_positions_diff_lines(start_positions, end_positions)
        if diff_lines:
            month_text += "\n\n📦 Изменения позиций за месяц\n" + "\n".join(diff_lines)

    return month_text


def _instrument_type_to_group(instr_type: str | None) -> str:
    """
    Группировка типов инструментов в человекочитаемые категории.
    """
    if not instr_type:
        return "Другое"

    t = instr_type.lower()
    if "share" in t or "stock" in t:
        return "Акции"
    if "bond" in t:
        return "Облигации"
    if "etf" in t or "fund" in t:
        return "ETF"
    if "currency" in t:
        return "Валюта"
    if "futures" in t or "future" in t:
        return "Фьючерсы"
    return "Другое"


def quantize_ruble_amount(value: Decimal | float | int | None) -> Decimal:
    return normalize_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def format_decimal_number(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    decimal_value = normalize_decimal(value)
    quantizer = Decimal("1") if precision == 0 else Decimal(f"1.{'0' * precision}")
    quantized = decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
    text_value = format(quantized, "f")
    if "." in text_value:
        text_value = text_value.rstrip("0").rstrip(".")
    if signed and quantized >= 0:
        text_value = f"+{text_value}"
    return text_value


def format_decimal_pct(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} %"


def format_decimal_pp(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} п.п."


def format_rebalance_weight(value: Decimal | float | int | None) -> str:
    decimal_value = normalize_decimal(value).quantize(Decimal("1.0"), rounding=ROUND_HALF_UP)
    return f"{format(decimal_value, '.1f').replace('.', ',')}%"


def format_human_date_ru(value: date | None) -> str:
    if value is None:
        return ""
    return f"{value.day} {MONTHS_RU_GENITIVE[value.month]} {value.year}"


def parse_decimal_input(raw_value: str, *, allow_zero: bool = True) -> Decimal:
    cleaned = raw_value.strip().replace(" ", "").replace(",", ".")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if cleaned.endswith("₽"):
        cleaned = cleaned[:-1]
    if not cleaned:
        raise ValueError("Пустое значение недопустимо.")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Не удалось разобрать число: {raw_value}") from exc
    if not allow_zero and value <= 0:
        raise ValueError("Сумма должна быть положительной.")
    return value


def parse_rebalance_targets_args(args: list[str]) -> dict[str, Decimal]:
    if not args:
        raise ValueError(TARGETS_USAGE_TEXT)

    targets: dict[str, Decimal] = {}
    for token in args:
        if "=" not in token:
            raise ValueError(TARGETS_USAGE_TEXT)
        raw_key, raw_value = token.split("=", 1)
        key = raw_key.strip().lower()
        asset_class = REBALANCE_TARGET_ALIASES.get(key)
        if asset_class is None:
            raise ValueError(f"Неизвестный класс `{raw_key}`. Поддерживаются: stocks, bonds, etf, cash.")
        if asset_class in targets:
            raise ValueError(f"Класс `{asset_class}` указан несколько раз.")

        value = parse_decimal_input(raw_value)
        if value < 0:
            raise ValueError("Таргеты не могут быть отрицательными.")
        targets[asset_class] = value

    normalized = {
        asset_class: targets.get(asset_class, Decimal("0"))
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    if sum(normalized.values()) != Decimal("100"):
        raise ValueError("Сумма таргетов должна быть ровно 100.")
    return normalized


def aggregate_rebalance_values_by_class(
    positions: list[dict] | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    class_values = {
        asset_class: Decimal("0")
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    other_groups: dict[str, Decimal] = {}

    for pos in positions or []:
        group_name = _instrument_type_to_group(pos.get("instrument_type"))
        position_value = normalize_decimal(pos.get("position_value"))
        asset_class = REBALANCE_GROUP_TO_CLASS.get(group_name)
        if asset_class is not None:
            class_values[asset_class] += position_value
            continue
        other_groups[group_name] = other_groups.get(group_name, Decimal("0")) + position_value

    return class_values, other_groups


def compute_rebalance_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
) -> dict[str, Decimal | list[dict[str, Decimal | str]]]:
    rebalanceable_base = sum(class_values.get(asset_class, Decimal("0")) for asset_class in REBALANCE_ASSET_CLASSES)
    rows: list[dict[str, Decimal | str]] = []

    for asset_class in REBALANCE_ASSET_CLASSES:
        current_value = normalize_decimal(class_values.get(asset_class))
        target_pct = normalize_decimal(target_weights.get(asset_class))
        current_pct = (
            current_value * Decimal("100") / rebalanceable_base
            if rebalanceable_base > 0
            else Decimal("0")
        )
        delta_pct = current_pct - target_pct
        target_value = rebalanceable_base * target_pct / Decimal("100")
        delta_value = target_value - current_value
        rows.append(
            {
                "asset_class": asset_class,
                "label": REBALANCE_CLASS_LABELS[asset_class],
                "current_value": current_value,
                "current_pct": current_pct,
                "target_pct": target_pct,
                "delta_pct": delta_pct,
                "target_value": target_value,
                "delta_value": delta_value,
                "status": "в норме" if abs(delta_pct) <= REBALANCE_TOLERANCE_PCT else "вне нормы",
            }
        )

    return {
        "rebalanceable_base": rebalanceable_base,
        "rows": rows,
    }


def compute_invest_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
    deposit_amount: Decimal | float | int,
) -> dict[str, Decimal | dict[str, Decimal]]:
    rounded_deposit = quantize_ruble_amount(deposit_amount)
    if rounded_deposit <= 0:
        raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")

    rebalanceable_base = sum(class_values.get(asset_class, Decimal("0")) for asset_class in REBALANCE_ASSET_CLASSES)
    deficits = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}
    raw_allocations = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}

    if rebalanceable_base <= 0:
        for asset_class in REBALANCE_ASSET_CLASSES:
            raw_allocations[asset_class] = rounded_deposit * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
            deficits[asset_class] = raw_allocations[asset_class]
    else:
        new_base = rebalanceable_base + rounded_deposit
        for asset_class in REBALANCE_ASSET_CLASSES:
            desired_value = new_base * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
            deficits[asset_class] = max(desired_value - normalize_decimal(class_values.get(asset_class)), Decimal("0"))

        total_deficit = sum(deficits.values())
        if total_deficit > 0:
            for asset_class in REBALANCE_ASSET_CLASSES:
                raw_allocations[asset_class] = rounded_deposit * deficits[asset_class] / total_deficit
        else:
            for asset_class in REBALANCE_ASSET_CLASSES:
                raw_allocations[asset_class] = rounded_deposit * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
                deficits[asset_class] = raw_allocations[asset_class]

    allocations = {
        asset_class: quantize_ruble_amount(raw_allocations.get(asset_class))
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    residue = rounded_deposit - sum(allocations.values())
    if residue != 0:
        residue_asset_class = max(
            REBALANCE_ASSET_CLASSES,
            key=lambda asset_class: (
                deficits.get(asset_class, Decimal("0")),
                normalize_decimal(target_weights.get(asset_class)),
            ),
        )
        allocations[residue_asset_class] += residue

    return {
        "deposit_amount": rounded_deposit,
        "rebalanceable_base": rebalanceable_base,
        "deficits": deficits,
        "allocations": allocations,
    }


def get_rebalance_targets(session, account_id: str) -> dict[str, Decimal] | None:
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT asset_class, target_weight_pct
                    FROM rebalance_targets
                    WHERE account_id = :account_id
                    """
                ),
                {"account_id": account_id},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "rebalance_targets"):
            session.rollback()
            return None
        raise

    if not rows:
        return {}

    targets = {
        asset_class: Decimal("0")
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    for row in rows:
        asset_class = row["asset_class"]
        if asset_class in targets:
            targets[asset_class] = normalize_decimal(row["target_weight_pct"])
    return targets


def replace_rebalance_targets(session, account_id: str, targets: dict[str, Decimal]) -> bool:
    try:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        session.execute(
            text("DELETE FROM rebalance_targets WHERE account_id = :account_id"),
            {"account_id": account_id},
        )
        session.execute(
            text(
                """
                INSERT INTO rebalance_targets (
                    account_id,
                    asset_class,
                    target_weight_pct,
                    created_at,
                    updated_at
                )
                VALUES (
                    :account_id,
                    :asset_class,
                    :target_weight_pct,
                    :created_at,
                    :updated_at
                )
                """
            ),
            [
                {
                    "account_id": account_id,
                    "asset_class": asset_class,
                    "target_weight_pct": decimal_to_str(targets[asset_class]),
                    "created_at": now_utc,
                    "updated_at": now_utc,
                }
                for asset_class in REBALANCE_ASSET_CLASSES
            ],
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "rebalance_targets"):
            return False
        raise
    return True


def bootstrap_invest_notifications(session, account_id: str) -> bool:
    try:
        existing_count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM invest_notifications
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        ).scalar_one()
    except Exception as exc:
        if _is_undefined_table_error(exc, "invest_notifications"):
            session.rollback()
            return False
        raise

    if existing_count:
        return True

    bootstrap_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=2)
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            INSERT INTO invest_notifications (
                account_id,
                operation_id,
                operation_date,
                amount,
                created_at
            )
            SELECT
                operations_dedup.account_id,
                operations_dedup.operation_id,
                operations_dedup.date,
                ABS(operations_dedup.amount),
                :created_at
            FROM operations_dedup
            WHERE operations_dedup.account_id = :account_id
              AND operations_dedup.operation_type IN :operation_types
              AND operations_dedup.state = :executed_state
              AND operations_dedup.date < :bootstrap_cutoff
            ON CONFLICT (account_id, operation_id) DO NOTHING
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": DEPOSIT_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
            "bootstrap_cutoff": bootstrap_cutoff,
            "created_at": created_at,
        },
    )
    session.commit()
    return True


def get_pending_invest_notifications(session, account_id: str) -> list[dict] | None:
    bootstrapped = bootstrap_invest_notifications(session, account_id)
    if not bootstrapped:
        return None

    try:
        rows = (
            session.execute(
                text(
                    f"""
                    {OPERATIONS_DEDUP_CTE}
                    SELECT
                        operations_dedup.operation_id,
                        operations_dedup.date,
                        ABS(operations_dedup.amount) AS amount
                    FROM operations_dedup
                    LEFT JOIN invest_notifications notified
                      ON notified.account_id = operations_dedup.account_id
                     AND notified.operation_id = operations_dedup.operation_id
                    WHERE operations_dedup.account_id = :account_id
                      AND operations_dedup.operation_type IN :operation_types
                      AND operations_dedup.state = :executed_state
                      AND notified.operation_id IS NULL
                    ORDER BY operations_dedup.date ASC
                    """
                ).bindparams(bindparam("operation_types", expanding=True)),
                {
                    "account_id": account_id,
                    "operation_types": DEPOSIT_OPERATION_TYPES,
                    "executed_state": EXECUTED_OPERATION_STATE,
                },
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "invest_notifications"):
            session.rollback()
            return None
        raise
    return rows


def mark_invest_notification_sent(
    session,
    *,
    account_id: str,
    operation_id: str,
    operation_date: datetime,
    amount: Decimal,
) -> bool:
    try:
        created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.execute(
            text(
                """
                INSERT INTO invest_notifications (
                    account_id,
                    operation_id,
                    operation_date,
                    amount,
                    created_at
                )
                VALUES (
                    :account_id,
                    :operation_id,
                    :operation_date,
                    :amount,
                    :created_at
                )
                ON CONFLICT (account_id, operation_id) DO NOTHING
                """
            ),
            {
                "account_id": account_id,
                "operation_id": operation_id,
                "operation_date": operation_date,
                "amount": decimal_to_str(amount),
                "created_at": created_at,
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "invest_notifications"):
            return False
        raise
    return True


def get_latest_rebalance_snapshot(session, account_id: str) -> dict:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    positions: list[dict] = []
    snapshot_date: date | None = None
    total_portfolio_value = Decimal("0")
    if latest_snapshot:
        snapshot_date = latest_snapshot["snapshot_date"]
        total_portfolio_value = normalize_decimal(latest_snapshot["total_value"])
        positions = get_positions_for_snapshot(session, latest_snapshot["id"])

    class_values, other_groups = aggregate_rebalance_values_by_class(positions)
    if total_portfolio_value <= 0:
        total_portfolio_value = sum(class_values.values()) + sum(other_groups.values())

    return {
        "snapshot_date": snapshot_date,
        "total_portfolio_value": total_portfolio_value,
        "class_values": class_values,
        "other_groups": other_groups,
    }


def _build_rebalance_diff_lines(rows: list[dict[str, Decimal | str]]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"- {'✅' if row['status'] == 'в норме' else '⚠️'} "
            f"{row['label']}: {format_rebalance_weight(row['current_pct'])} / "
            f"{format_rebalance_weight(row['target_pct'])}"
        )
    return lines


def _build_out_of_model_lines(
    other_groups: dict[str, Decimal],
    total_portfolio_value: Decimal,
) -> list[str]:
    if not other_groups:
        return []

    lines = ["Вне модели:"]
    sorted_groups = sorted(other_groups.items(), key=lambda item: item[1], reverse=True)
    for group_name, group_value in sorted_groups:
        share_pct = (
            group_value * Decimal("100") / total_portfolio_value
            if total_portfolio_value > 0
            else Decimal("0")
        )
        lines.append(
            f"- {group_name}: {fmt_decimal_rub(group_value, precision=0)} "
            f"({format_decimal_pct(share_pct, precision=1)} портфеля)"
        )
    return lines


def build_targets_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "🎯 Таргеты аллокации"
    if snapshot_date is not None:
        header += f" (на {snapshot_date.isoformat()})"

    lines = [header, "", "Текущие таргеты (факт / план):"]
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    if snapshot_date is None:
        lines.append("")
        lines.append("Фактическая структура появится после первого снапшота.")

    return "\n".join(lines)


def build_rebalance_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "⚖️ Ребаланс"
    lines = [header]
    if snapshot_date is not None:
        lines.extend(["", format_human_date_ru(snapshot_date)])
    lines.extend(["", "Текущие таргеты (факт / план):"])
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    sell_rows: list[tuple[str, Decimal]] = []
    buy_rows: list[tuple[str, Decimal]] = []
    for row in rebalance_plan["rows"]:
        delta_value = quantize_ruble_amount(row["delta_value"])
        if delta_value > 0:
            buy_rows.append((row["label"], delta_value))
        elif delta_value < 0:
            sell_rows.append((row["label"], abs(delta_value)))

    sell_rows.sort(key=lambda item: item[1], reverse=True)
    buy_rows.sort(key=lambda item: item[1], reverse=True)

    lines.append("")
    lines.append("Чтобы поймать баланс сейчас:")
    if sell_rows:
        lines.extend(["", "📉 Продать:"])
        for label, amount in sell_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if buy_rows:
        lines.extend(["", "📈 Купить:"])
        for label, amount in buy_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if not sell_rows and not buy_rows:
        lines.append("")
        lines.append("Баланс уже близок к целевому, действий не требуется.")

    if snapshot["other_groups"]:
        lines.append("")
        lines.append(
            "Расчёт buy/sell сделан по ребалансируемой части портфеля: "
            f"{fmt_decimal_rub(rebalance_plan['rebalanceable_base'], precision=0)}."
        )

    return "\n".join(lines)


def build_invest_text_for_account(
    session,
    account_id: str,
    deposit_amount: Decimal | float | int,
    *,
    header: str | None = None,
) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    invest_plan = compute_invest_plan(snapshot["class_values"], targets, deposit_amount)

    lines = [header or f"💸 Как распределить пополнение {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}"]
    lines.append("")
    lines.append("Текущие таргеты (факт / план):")
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    lines.append("")
    lines.append(f"Распределение пополнения {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}:")
    for asset_class in REBALANCE_ASSET_CLASSES:
        allocation = invest_plan["allocations"][asset_class]
        lines.append(
            f"- {REBALANCE_CLASS_LABELS[asset_class]}: {fmt_decimal_rub(allocation, precision=0)}"
        )

    if snapshot["other_groups"]:
        lines.append("")
        lines.append("Классы вне модели не участвуют в распределении пополнения.")

    return "\n".join(lines)


def build_structure_text() -> str:
    """
    Структура портфеля по последнему снапшоту, с разбивкой по типам:

    - сводка по типам (тип / сумма / доля / P&L)
    - далее блоки по типам (ETF, акции, валюта и т.д.), отсортированные по сумме
    - внутри блока — бумаги по убыванию суммы
    """
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        snap = get_latest_snapshot_with_id(session, account_id)
        if not snap:
            return "Нет ни одного снапшота портфеля."

        positions = get_positions_for_snapshot(session, snap["id"])

    if not positions:
        return "В последнем снапшоте нет позиций."

    snap_date: date = snap["snapshot_date"]
    total_value = float(snap["total_value"])

    # Группируем по типам
    groups: dict[str, dict] = {}
    for pos in positions:
        instr_type = pos.get("instrument_type")
        group_name = _instrument_type_to_group(instr_type)

        qty = float(pos["quantity"]) if pos["quantity"] is not None else 0.0
        price = float(pos["current_price"]) if pos["current_price"] is not None else 0.0
        value = float(pos["position_value"]) if pos["position_value"] is not None else 0.0
        pl = float(pos["expected_yield"]) if pos["expected_yield"] is not None else 0.0
        pl_pct = float(pos["expected_yield_pct"]) if pos["expected_yield_pct"] is not None else 0.0
        weight = float(pos["weight_pct"]) if pos["weight_pct"] is not None else None

        ticker = pos["ticker"] or pos["figi"]
        name = pos["name"] or ticker

        if group_name not in groups:
            groups[group_name] = {
                "total_value": 0.0,
                "total_pl": 0.0,
                "positions": [],
            }

        groups[group_name]["total_value"] += value
        groups[group_name]["total_pl"] += pl
        groups[group_name]["positions"].append(
            {
                "ticker": ticker,
                "name": name,
                "qty": qty,
                "price": price,
                "value": value,
                "pl": pl,
                "pl_pct": pl_pct,
                "weight": weight,
            }
        )

    # Считаем доли и P&L% по группам
    group_list = []
    for g_name, g in groups.items():
        g_val = g["total_value"]
        g_pl = g["total_pl"]
        share_pct = g_val / total_value * 100.0 if total_value > 0 else 0.0
        pl_pct = g_pl / g_val * 100.0 if g_val > 0 else 0.0
        group_list.append(
            {
                "name": g_name,
                "value": g_val,
                "pl": g_pl,
                "share_pct": share_pct,
                "pl_pct": pl_pct,
                "positions": g["positions"],
            }
        )

    # Сортируем группы по убыванию суммы
    group_list.sort(key=lambda x: x["value"], reverse=True)

    # Внутри каждой группы позиции по убыванию суммы
    for g in group_list:
        g["positions"].sort(key=lambda p: p["value"], reverse=True)

    lines: list[str] = []
    lines.append(
        f"📂 Структура портфеля *{ACCOUNT_FRIENDLY_NAME}* "
        f"(на {snap_date.isoformat()})"
    )
    lines.append("")
    lines.append("Сводка по типам:")

    for g in group_list:
        lines.append(
            f"- {g['name']} — {fmt_rub(g['value'])} "
            f"({g['share_pct']:.1f} % портфеля), "
            f"P&L: {fmt_rub(g['pl'])} ({g['pl_pct']:+.1f} %)"
        )

    lines.append("")
    # Небольшой текстовый переход, в стиле выбранного варианта
    lines.append("По сути сейчас структура выглядит так:")

    for g in group_list:
        lines.append("")
        lines.append(f"{g['name']}:")
        for p in g["positions"]:
            name = p["name"]
            ticker = p["ticker"]
            qty = p["qty"]
            price = p["price"]
            value = p["value"]
            pl = p["pl"]
            pl_pct = p["pl_pct"]

            qty_str = f"{qty:,.0f}".replace(",", " ")
            price_str = fmt_rub(price, precision=2)
            value_str = fmt_rub(value, precision=0)
            pl_str = fmt_rub(pl, precision=0)
            pl_pct_str = f"{pl_pct:+.1f} %"

            lines.append(f"- {name} [{ticker}]")
            lines.append(
                f"  {price_str} × {qty_str} шт = {value_str} / доход: {pl_str} ({pl_pct_str})"
            )

    # Итог по портфелю
    total_pl = sum(g["pl"] for g in group_list)
    total_pl_pct = total_pl / total_value * 100.0 if total_value > 0 else 0.0

    lines.append("")
    lines.append("Итог:")
    lines.append(f"- Общая стоимость портфеля: *{fmt_rub(total_value)}*")
    lines.append(
        f"- Совокупный результат по всем бумагам: "
        f"{fmt_rub(total_pl)} ({total_pl_pct:+.1f} %)"
    )

    return "\n".join(lines)


def build_history_chart(path: str) -> str | None:
    """
    Строит PNG-график стоимости портфеля и накопленных пополнений.
    Ось X — понедельная разметка.
    """
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        ts = get_portfolio_timeseries(session, account_id)
        deps = get_deposits_by_date(session, account_id)

    if len(ts) < 2:
        return None

    # Сортируем исходные данные (на всякий случай)
    ts_sorted = sorted(ts, key=lambda x: x["snapshot_date"])
    deps_sorted = sorted(deps, key=lambda x: x["d"])

    start_date = ts_sorted[0]["snapshot_date"]
    end_date = ts_sorted[-1]["snapshot_date"]

    # Генерируем даты с шагом 1 неделя
    week_dates = []
    curr = start_date
    while curr <= end_date:
        week_dates.append(curr)
        curr += timedelta(days=7)
    
    # Если последняя точка далеко от end_date, можно добавить и end_date
    if week_dates[-1] < end_date:
        week_dates.append(end_date)

    # Подготавливаем массивы значений для графика
    values = []
    cum_deps = []

    # Превращаем ts_sorted в список кортежей для удобства
    ts_data = [(row["snapshot_date"], float(row["total_value"])) for row in ts_sorted]
    # Превращаем deps_sorted в список кортежей
    deps_data = [(row["d"], float(row["s"])) for row in deps_sorted]

    for d in week_dates:
        # 1. Стоимость портфеля на дату d (берем последний снапшот <= d)
        val = 0.0
        relevant_snaps = [v for (dt, v) in ts_data if dt <= d]
        if relevant_snaps:
            val = relevant_snaps[-1]
        values.append(val)

        # 2. Кумулятивные пополнения на дату d (сумма всех депозитов <= d)
        relevant_deps = [amt for (dt, amt) in deps_data if dt <= d]
        total_d = sum(relevant_deps)
        cum_deps.append(total_d)

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(
        fig,
        f"{ACCOUNT_FRIENDLY_NAME}: портфель и пополнения",
        "Разница между линиями показывает результат сверх пополнений.",
    )
    apply_chart_style(ax, rub_axis_formatter)

    if cum_deps:
        positive_gap = [value >= dep for value, dep in zip(values, cum_deps)]
        negative_gap = [value < dep for value, dep in zip(values, cum_deps)]
        ax.fill_between(
            week_dates,
            values,
            cum_deps,
            where=positive_gap,
            color=CHART_COLORS["positive_fill"],
            alpha=0.8,
            interpolate=True,
            zorder=1,
        )
        ax.fill_between(
            week_dates,
            values,
            cum_deps,
            where=negative_gap,
            color=CHART_COLORS["negative_fill"],
            alpha=0.8,
            interpolate=True,
            zorder=1,
        )
        ax.plot(
            week_dates,
            cum_deps,
            color=CHART_COLORS["deposits"],
            linewidth=2.0,
            linestyle=(0, (4, 3)),
            zorder=2,
        )

    ax.plot(
        week_dates,
        values,
        color=CHART_COLORS["portfolio"],
        linewidth=2.6,
        zorder=3,
    )

    tick_dates, tick_labels = build_date_ticks(week_dates, max_ticks=7)
    ax.set_xticks(tick_dates)
    ax.set_xticklabels(tick_labels)
    ax.set_ylabel("Стоимость")
    ax.margins(x=0.03, y=0.14)

    portfolio_offset = 12
    deposits_offset = -14 if values[-1] >= cum_deps[-1] else 12
    if abs(values[-1] - cum_deps[-1]) < max(values[-1], cum_deps[-1], 1.0) * 0.07:
        portfolio_offset = 18
        deposits_offset = -18

    annotate_series_last_point(
        ax,
        week_dates,
        values,
        f"Портфель {fmt_compact_rub(values[-1])}",
        CHART_COLORS["portfolio"],
        y_offset=portfolio_offset,
    )
    if cum_deps:
        annotate_series_last_point(
            ax,
            week_dates,
            cum_deps,
            f"Пополнения {fmt_compact_rub(cum_deps[-1])}",
            CHART_COLORS["deposits"],
            y_offset=deposits_offset,
        )

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_year_chart(path: str, year: int, end_date_exclusive: date) -> str | None:
    year_start = date(year, 1, 1)
    period_start_dt = datetime.combine(year_start, time.min)
    period_end_dt_exclusive = datetime.combine(end_date_exclusive, time.min)
    is_ytd = end_date_exclusive.year == year and end_date_exclusive <= (datetime.now(TZ).date() + timedelta(days=1))

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        portfolio_rows = get_monthly_portfolio_values(session, account_id, period_start_dt, period_end_dt_exclusive, is_ytd)
        deposits_rows = get_monthly_deposits(session, account_id, period_start_dt, period_end_dt_exclusive)

    if not portfolio_rows:
        return None

    portfolio_by_month = {
        row["month_start"]: float(row["total_value"] or 0)
        for row in portfolio_rows
    }
    deposits_by_month = {
        row["month_start"]: float(row["amount"] or 0)
        for row in deposits_rows
    }

    months = sorted(portfolio_by_month.keys())
    portfolio_values = [portfolio_by_month[m] for m in months]
    deposits_values = [deposits_by_month.get(m, 0.0) for m in months]

    x = list(range(len(months)))
    chart_title = f"{year} YTD: как рос портфель" if is_ytd else f"{year}: как рос портфель"
    has_deposits = any(value != 0 for value in deposits_values)
    chart_subtitle = (
        "Сверху — стоимость на конец месяца, снизу — пополнения за месяц."
        if has_deposits
        else "Пополнений за этот период не было, поэтому показана только динамика портфеля."
    )

    if has_deposits:
        fig, (ax_portfolio, ax_deposits) = plt.subplots(
            2,
            1,
            figsize=(10.5, 5.9),
            sharex=True,
            gridspec_kw={"height_ratios": [2.2, 1.25]},
        )
    else:
        fig, ax_portfolio = plt.subplots(figsize=(10.5, 4.6))
        ax_deposits = None
    set_chart_header(
        fig,
        chart_title,
        chart_subtitle,
    )

    apply_chart_style(ax_portfolio, rub_axis_formatter)
    if ax_deposits is not None:
        apply_chart_style(ax_deposits, rub_axis_formatter)

    if ax_deposits is not None:
        ax_portfolio.fill_between(x, portfolio_values, color=CHART_COLORS["portfolio_fill"], alpha=0.65, zorder=1)
    ax_portfolio.plot(
        x,
        portfolio_values,
        color=CHART_COLORS["portfolio"],
        linewidth=2.6,
        marker="o",
        markersize=4,
        zorder=3,
    )
    ax_portfolio.set_ylabel("Портфель")
    ax_portfolio.margins(x=0.04)
    if ax_deposits is None:
        set_value_axis_limits(ax_portfolio, portfolio_values, min_padding_ratio=0.18, flat_padding_ratio=0.03)
    else:
        ax_portfolio.margins(y=0.16)
    annotate_series_last_point(
        ax_portfolio,
        x,
        portfolio_values,
        f"{fmt_compact_rub(portfolio_values[-1])}",
        CHART_COLORS["portfolio"],
        y_offset=12,
    )

    labels = build_month_tick_labels(months)
    if ax_deposits is not None:
        ax_deposits.bar(
            x,
            deposits_values,
            width=0.58,
            color=CHART_COLORS["deposits"],
            edgecolor="none",
            zorder=3,
        )
        ax_deposits.set_ylabel("Пополнения")
        ax_deposits.margins(x=0.04)
        max_deposit = max(deposits_values) if deposits_values else 0.0
        upper_limit = max_deposit * 1.2 if max_deposit > 0 else 1.0
        ax_deposits.set_ylim(0, upper_limit)
        annotate_bar_values(
            ax_deposits,
            x,
            deposits_values,
            lambda value: fmt_compact_rub(value, precision=0),
            text_color=CHART_COLORS["muted"],
        )
        ax_deposits.set_xticks(x)
        ax_deposits.set_xticklabels(labels)
        fig.tight_layout(rect=(0, 0, 1, 0.86), h_pad=1.2)
    else:
        ax_portfolio.set_xticks(x)
        ax_portfolio.set_xticklabels(labels)
        fig.tight_layout(rect=(0, 0, 1, 0.83))

    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_year_monthly_delta_chart(path: str, year: int, end_date_exclusive: date) -> str | None:
    year_start = date(year, 1, 1)
    period_start_dt = datetime.combine(year_start, time.min)
    period_end_dt_exclusive = datetime.combine(end_date_exclusive, time.min)
    is_ytd = end_date_exclusive.year == year and end_date_exclusive <= (datetime.now(TZ).date() + timedelta(days=1))

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        portfolio_rows = get_monthly_portfolio_values(session, account_id, period_start_dt, period_end_dt_exclusive, is_ytd)
        deposits_rows = get_monthly_deposits(session, account_id, period_start_dt, period_end_dt_exclusive)

        if not portfolio_rows:
            return None

        deposits_by_month = {
            row["month_start"]: float(row["amount"] or 0)
            for row in deposits_rows
        }

        months = [row["month_start"] for row in portfolio_rows]
        values = [float(row["total_value"] or 0) for row in portfolio_rows]

        delta_points: list[tuple[date, float]] = []
        first_month_start = months[0]
        first_month_end_exclusive = (
            date(first_month_start.year + 1, 1, 1)
            if first_month_start.month == 12
            else date(first_month_start.year, first_month_start.month + 1, 1)
        )

        first_snapshot = get_first_snapshot_in_period(session, account_id, first_month_start, first_month_end_exclusive)
        first_month_base = float(first_snapshot["total_value"] or 0) if first_snapshot is not None else values[0]
        first_period_start = first_snapshot["snapshot_date"] if first_snapshot is not None else first_month_start

        if first_month_start.month == 1:
            prev_snapshot = get_last_snapshot_before_date(session, account_id, first_month_start)
            if prev_snapshot is not None:
                first_month_base = float(prev_snapshot["total_value"] or 0)
                first_period_start = first_month_start

        if first_period_start == first_month_start:
            first_month_deposits = deposits_by_month.get(first_month_start, 0.0)
        else:
            first_month_deposits = get_deposits_sum_for_period(
                session,
                account_id,
                datetime.combine(first_period_start, time.min),
                datetime.combine(first_month_end_exclusive, time.min),
            )

        first_month_delta = values[0] - first_month_base - first_month_deposits
        delta_points.append((months[0], first_month_delta))

        for idx in range(1, len(months)):
            month_start = months[idx]
            month_deposits = deposits_by_month.get(month_start, 0.0)
            month_delta = values[idx] - values[idx - 1] - month_deposits
            delta_points.append((month_start, month_delta))

    if not delta_points:
        return None

    month_labels = [month for month, _ in delta_points]
    deltas = [delta for _, delta in delta_points]
    x = list(range(len(month_labels)))

    title_prefix = f"{year} YTD" if is_ytd else str(year)
    best_idx = max(range(len(deltas)), key=lambda idx: deltas[idx])
    worst_idx = min(range(len(deltas)), key=lambda idx: deltas[idx])
    subtitle = (
        f"Без пополнений. Лучший месяц: {format_month_short_label(month_labels[best_idx])} "
        f"{fmt_compact_rub(deltas[best_idx], signed=True)}, "
        f"худший: {format_month_short_label(month_labels[worst_idx])} "
        f"{fmt_compact_rub(deltas[worst_idx], signed=True)}."
    )

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(fig, f"{title_prefix}: результат по месяцам", subtitle)
    apply_chart_style(ax, rub_axis_formatter)

    bar_colors = []
    for delta in deltas:
        if delta > 0:
            bar_colors.append(CHART_COLORS["positive"])
        elif delta < 0:
            bar_colors.append(CHART_COLORS["negative"])
        else:
            bar_colors.append(CHART_COLORS["neutral"])

    ax.bar(x, deltas, width=0.62, color=bar_colors, edgecolor="none", zorder=3)
    ax.axhline(0, linewidth=1, color=CHART_COLORS["spine"], zorder=2)
    ax.set_ylabel("Результат")
    ax.set_xticks(x)
    ax.set_xticklabels(build_month_tick_labels(month_labels))
    ax.margins(x=0.04, y=0.18)
    annotate_bar_values(
        ax,
        x,
        deltas,
        lambda value: fmt_compact_rub(value, signed=True),
    )

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def _format_asset_lines(rows: list[dict], total: Decimal, title: str, top_n: int = YEAR_REPORT_TOP_N) -> list[str]:
    lines = [title]
    for row in rows[:top_n]:
        ticker = row.get("ticker") or "—"
        name = row.get("name") or row.get("figi") or "—"
        lines.append(f"• {name} ({ticker})  {fmt_decimal_rub(row.get('amount'))}")

    lines.append(f"Итого: {fmt_decimal_rub(total)}")
    return lines


def build_year_summary(year: int | None) -> tuple[str, str, str | None]:
    period_start_dt, period_end_dt_exclusive, label, _ = get_year_period(year)
    period_start = period_start_dt.date()
    period_end_inclusive = period_end_dt_exclusive.date() - timedelta(days=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        year_financials = get_year_financials_from_operations(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )

        start_snap, end_snap = get_period_snapshots(session, account_id, period_start, period_end_dt_exclusive.date())
        diff_lines, diff_error = compute_positions_diff_grouped(session, account_id, period_start_dt, period_end_dt_exclusive)
        realized_by_asset, realized_total = compute_realized_by_asset(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        income_by_asset_net, income_total_net = compute_income_by_asset_net(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        unrealized = get_unrealized_at_period_end(session, account_id, period_end_dt_exclusive)

    dep_year = float(year_financials["deposits"])
    income_total_net = year_financials["income_net"]

    current_value = float(end_snap["total_value"]) if end_snap else 0.0
    delta_abs = None
    delta_pct = None
    if start_snap and end_snap:
        start_val = float(start_snap["total_value"])
        end_val = float(end_snap["total_value"])
        delta_abs = end_val - start_val
        if start_val != 0:
            delta_pct = delta_abs / start_val * 100.0

    plan = PLAN_ANNUAL_CONTRIB_RUB
    plan_pct = dep_year / plan * 100.0 if plan > 0 else 0.0

    if start_snap and end_snap:
        delta_line = f"Изменение стоимости: {fmt_rub(delta_abs)} ({fmt_pct(delta_pct, precision=2) if delta_pct is not None else '—'})"
    else:
        delta_line = "Изменение стоимости: нет данных (нет снапшота в начале периода)"

    summary_lines = [
        f"📅 *Команда /year {period_start.year}*",
        f"Период: {period_start.strftime('%d.%m.%Y')} — {period_end_inclusive.strftime('%d.%m.%Y')} ({label})",
        "",
        f"Стоимость портфеля на конец периода: *{fmt_rub(current_value)}*",
        delta_line,
        f"Прогресс годового плана: {plan_pct:.1f} % ({fmt_rub(dep_year)} / {fmt_rub(plan)})",
        "",
        f"По открытым позициям: {fmt_decimal_rub(unrealized)}",
        "",
    ]
    if realized_by_asset or realized_total != 0:
        summary_lines.extend(_format_asset_lines(realized_by_asset, realized_total, "💰 Реализовано"))

    if income_by_asset_net or income_total_net != 0:
        if summary_lines and summary_lines[-1] != "":
            summary_lines.append("")
        summary_lines.extend(_format_asset_lines(income_by_asset_net, income_total_net, "🧾 Дивиденды/купоны"))

    while summary_lines and summary_lines[-1] == "":
        summary_lines.pop()

    summary_text = "\n".join(summary_lines)

    if diff_error:
        diff_text = f"📦 {diff_error}"
    elif diff_lines:
        diff_text = f"📦 Изменения позиций за {label}\n\n" + "\n".join(diff_lines)
    else:
        diff_text = f"📦 За период {label} изменений по позициям не найдено."

    return summary_text, diff_text, label


def build_net_external_flow_by_day(external_cashflows: list[dict]) -> dict[date, float]:
    net_external_flow_by_day: dict[date, float] = {}
    for row in external_cashflows:
        dt = row.get("date")
        local_date = to_local_market_date(dt)
        if local_date is None:
            continue

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if operation_type in DEPOSIT_OPERATION_TYPES:
            signed_amount = amount
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            signed_amount = -amount
        else:
            continue

        net_external_flow_by_day[local_date] = net_external_flow_by_day.get(local_date, 0.0) + signed_amount

    return net_external_flow_by_day


def compute_twr_timeseries(session, account_id: str):
    snapshot_rows = get_portfolio_timeseries_agg_by_date(session, account_id)
    external_cashflows = get_external_cashflows_raw(session, account_id)
    net_external_flow_by_day = build_net_external_flow_by_day(external_cashflows)
    return compute_twr_series(snapshot_rows, net_external_flow_by_day)


def compute_portfolio_xirr_and_run_rate(
    session,
    account_id: str,
) -> tuple[float | None, float | None, date | None]:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None or latest_snapshot.get("total_value") is None:
        return None, None, None

    cashflows: list[tuple[datetime, float]] = []
    for row in get_external_cashflows_raw(session, account_id):
        dt = row.get("date")
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if operation_type in DEPOSIT_OPERATION_TYPES:
            cashflows.append((dt, -amount))
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            cashflows.append((dt, amount))

    if latest_snapshot["snapshot_at"] is not None:
        terminal_dt = latest_snapshot["snapshot_at"]
        if terminal_dt.tzinfo is None:
            terminal_dt = terminal_dt.replace(tzinfo=timezone.utc)
    else:
        terminal_dt = datetime.combine(
            latest_snapshot["snapshot_date"],
            time.max,
        ).replace(tzinfo=timezone.utc)

    current_value = float(latest_snapshot["total_value"])
    cashflows.append((terminal_dt, current_value))

    xirr_value = compute_xirr(cashflows)
    projection_date = date(latest_snapshot["snapshot_date"].year, 12, 31)
    projected_value = project_run_rate_value(
        current_value,
        xirr_value,
        latest_snapshot["snapshot_date"],
        projection_date,
    )
    return xirr_value, projected_value, projection_date


def render_twr_chart(path: str, dates: list[date], values: list[float | None], twr: list[float]) -> str:
    twr_pct = [x * 100.0 for x in twr]

    value_points = [(dt, value) for dt, value in zip(dates, values) if value is not None]
    value_dates = [dt for dt, _ in value_points]
    value_series = [float(value) for _, value in value_points]

    fig, (ax_value, ax_twr) = plt.subplots(
        2,
        1,
        figsize=(10.5, 5.9),
        sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.45]},
    )
    set_chart_header(
        fig,
        ACCOUNT_FRIENDLY_NAME + ": динамика и TWR",
        "Нижний график показывает доходность без искажения пополнениями.",
    )

    apply_chart_style(ax_value, rub_axis_formatter)
    apply_chart_style(ax_twr, pct_axis_formatter)

    if value_series:
        ax_value.fill_between(value_dates, value_series, color=CHART_COLORS["portfolio_fill"], alpha=0.65, zorder=1)
        ax_value.plot(
            value_dates,
            value_series,
            color=CHART_COLORS["portfolio"],
            linewidth=2.6,
            zorder=3,
        )
        ax_value.set_ylabel("Портфель")
        ax_value.margins(x=0.03, y=0.16)
        annotate_series_last_point(
            ax_value,
            value_dates,
            value_series,
            f"{fmt_compact_rub(value_series[-1])}",
            CHART_COLORS["portfolio"],
            y_offset=12,
        )

    positive_twr = [value >= 0 for value in twr_pct]
    negative_twr = [value < 0 for value in twr_pct]
    ax_twr.fill_between(
        dates,
        twr_pct,
        0,
        where=positive_twr,
        color=CHART_COLORS["positive_fill"],
        alpha=0.8,
        interpolate=True,
        zorder=1,
    )
    ax_twr.fill_between(
        dates,
        twr_pct,
        0,
        where=negative_twr,
        color=CHART_COLORS["negative_fill"],
        alpha=0.8,
        interpolate=True,
        zorder=1,
    )
    ax_twr.plot(
        dates,
        twr_pct,
        color=CHART_COLORS["twr"],
        linewidth=2.2,
        zorder=3,
    )
    ax_twr.axhline(0, linewidth=1, color=CHART_COLORS["spine"], zorder=2)
    ax_twr.set_ylabel("TWR")
    ax_twr.margins(x=0.03, y=0.18)
    annotate_series_last_point(
        ax_twr,
        dates,
        twr_pct,
        f"TWR {fmt_compact_pct(twr_pct[-1], signed=True)}",
        CHART_COLORS["twr"],
        y_offset=12 if twr_pct[-1] >= 0 else -12,
    )

    tick_dates, tick_labels = build_date_ticks(dates, max_ticks=7)
    ax_twr.set_xticks(tick_dates)
    ax_twr.set_xticklabels(tick_labels)

    fig.tight_layout(rect=(0, 0, 1, 0.9), h_pad=1.15)
    fig.savefig(path, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)

    return path


def build_dataset_export(session) -> tuple[dict, list[dict], list[dict], list[dict], list[dict]]:
    account_id = resolve_reporting_account_id(session)
    if account_id is None:
        raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    bounds = get_dataset_bounds(session, account_id)
    min_date = bounds["min_date"]
    max_date = bounds["max_date"]
    if min_date is None or max_date is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    daily_rows = get_daily_snapshot_rows(session, account_id)
    positions_rows = list(get_positions_for_snapshot(session, latest_snapshot["id"]))
    asset_alias_rows = list(get_asset_alias_rows(session))
    asset_alias_by_instrument_uid, asset_alias_by_figi = build_asset_alias_lookup(asset_alias_rows)
    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=account_id,
            start_dt=datetime.combine(min_date, time.min),
            end_dt=datetime.combine(max_date + timedelta(days=1), time.min),
        )
    )
    income_rows = list(get_income_events_for_period(session, account_id, min_date, max_date))
    twr_data = compute_twr_timeseries(session, account_id)

    twr_by_date: dict[date, float] = {}
    if twr_data is not None:
        dates, _values, twr_series = twr_data
        twr_by_date = {dt: round(value * 100.0, 6) for dt, value in zip(dates, twr_series)}

    deposits_by_day: dict[date, Decimal] = {}
    withdrawals_by_day: dict[date, Decimal] = {}
    commissions_by_day: dict[date, Decimal] = {}
    taxes_by_day: dict[date, Decimal] = {}
    operations_csv_rows: list[dict] = []
    unknown_operation_groups = 0
    mojibake_detected_count = 0

    for row in operations_rows:
        dt = row["date"]
        local_date = to_local_market_date(dt)
        group = classify_operation_group(row["operation_type"])
        if group == "other":
            unknown_operation_groups += 1

        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        description = row["description"]
        description_has_mojibake = has_mojibake(description)
        if description_has_mojibake:
            mojibake_detected_count += 1

        amount = normalize_decimal(row["amount"])
        amount_abs = abs(amount)
        if local_date is not None:
            if group == "deposit":
                deposits_by_day[local_date] = deposits_by_day.get(local_date, Decimal("0")) + amount
            elif group == "withdrawal":
                withdrawals_by_day[local_date] = withdrawals_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "commission":
                commissions_by_day[local_date] = commissions_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "income_tax" and not is_income_event_backed_tax_operation(row["operation_type"]):
                taxes_by_day[local_date] = taxes_by_day.get(local_date, Decimal("0")) + amount_abs

        operations_csv_rows.append(
            {
                "operation_id": row["operation_id"],
                "date_utc": to_iso_datetime(dt),
                "local_date": local_date.isoformat() if local_date is not None else None,
                "operation_type": row["operation_type"],
                "operation_group": group,
                "state": row["state"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "figi": figi,
                "name": row["name"],
                "amount": amount,
                "currency": row["currency"],
                "price": row["price"],
                "quantity": row["quantity"],
                "commission": row["commission"],
                "yield_amount": row["yield"],
                "description": description,
                "description_has_mojibake": description_has_mojibake,
                "source": row["source"],
            }
        )

    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}
    income_csv_rows: list[dict] = []
    for row in income_rows:
        event_date = row["event_date"]
        alias_row = asset_alias_by_figi.get(row["figi"]) if row.get("figi") else None
        asset_uid = alias_row.get("asset_uid") if alias_row is not None else None
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=alias_row.get("instrument_uid") if alias_row is not None else None,
            figi=row.get("figi"),
        )
        net_amount = normalize_decimal(row["net_amount"])
        tax_amount = normalize_decimal(row["tax_amount"])
        income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
        income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)
        income_csv_rows.append(
            {
                "event_date": event_date,
                "event_type": row["event_type"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "figi": row["figi"],
                "ticker": row["ticker"],
                "instrument_name": row["instrument_name"],
                "gross_amount": row["gross_amount"],
                "tax_amount": row["tax_amount"],
                "net_amount": row["net_amount"],
                "net_yield_pct": row["net_yield_pct"],
                "notified": row["notified"],
            }
        )

    daily_csv_rows: list[dict] = []
    previous_value: Decimal | None = None
    for row in daily_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row["total_value"])
        deposits = deposits_by_day.get(snapshot_date, Decimal("0"))
        withdrawals = withdrawals_by_day.get(snapshot_date, Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        commissions = commissions_by_day.get(snapshot_date, Decimal("0"))
        taxes = taxes_by_day.get(snapshot_date, Decimal("0"))
        income_tax = income_tax_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - taxes
        day_pnl = Decimal("0")
        if previous_value is not None:
            day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

        daily_csv_rows.append(
            {
                "date": snapshot_date,
                "snapshot_at_utc": to_iso_datetime(row["snapshot_at"]),
                "portfolio_value": portfolio_value,
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "deposits": deposits,
                "withdrawals": withdrawals,
                "income_net": income_net,
                "commissions": commissions,
                "operation_taxes": taxes,
                "income_taxes": income_tax,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": twr_by_date.get(snapshot_date),
            }
        )

    positions_csv_rows: list[dict] = []
    for row in positions_rows:
        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        positions_csv_rows.append(
            {
                "snapshot_date": latest_snapshot["snapshot_date"],
                "snapshot_at_utc": to_iso_datetime(latest_snapshot["snapshot_at"]),
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "position_uid": row["position_uid"],
                "figi": figi,
                "ticker": row["ticker"],
                "name": row["name"],
                "instrument_type": row["instrument_type"],
                "quantity": row["quantity"],
                "currency": row["currency"],
                "current_price": row["current_price"],
                "current_nkd": row["current_nkd"],
                "position_value": row["position_value"],
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "weight_pct": row["weight_pct"],
                "value_source": "quantity_x_current_price",
            }
        )

    deposits_total = sum((row["deposits"] for row in daily_csv_rows), Decimal("0"))
    withdrawals_total = sum((row["withdrawals"] for row in daily_csv_rows), Decimal("0"))
    income_net_total = sum((row["income_net"] for row in daily_csv_rows), Decimal("0"))
    commissions_total = sum((row["commissions"] for row in daily_csv_rows), Decimal("0"))
    operation_taxes_total = sum((row["operation_taxes"] for row in daily_csv_rows), Decimal("0"))
    income_taxes_total = sum((row["income_taxes"] for row in daily_csv_rows), Decimal("0"))
    current_value = normalize_decimal(latest_snapshot["total_value"])
    net_contributions = deposits_total - withdrawals_total
    period_start_value = normalize_decimal(daily_csv_rows[0]["portfolio_value"])
    period_end_value = current_value
    period_net_cashflow = sum((row["net_cashflow"] for row in daily_csv_rows[1:]), Decimal("0"))
    period_pnl_abs = period_end_value - period_start_value - period_net_cashflow
    has_full_history_from_zero = period_start_value == Decimal("0")
    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        latest_snapshot,
        positions_rows,
    )
    alias_groups_count = len({row["asset_uid"] for row in asset_alias_rows if row.get("asset_uid")})

    positions_missing_labels = sum(1 for row in positions_csv_rows if not (row["ticker"] or row["name"]))

    dataset = {
        "meta": {
            "dataset_version": 2,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "timezone": TZ_NAME,
            "account_name": ACCOUNT_FRIENDLY_NAME,
            "period_start": min_date.isoformat(),
            "period_end": max_date.isoformat(),
            "base_currency": latest_snapshot["currency"],
            "latest_snapshot_at": to_iso_datetime(latest_snapshot["snapshot_at"]),
        },
        "summary": {
            "current_value": current_value,
            "net_contributions": net_contributions,
            "deposits_total": deposits_total,
            "withdrawals_total": withdrawals_total,
            "income_net_total": income_net_total,
            "commissions_total": commissions_total,
            "income_taxes_total": income_taxes_total,
            "operation_taxes_total": operation_taxes_total,
            "taxes_total": income_taxes_total + operation_taxes_total,
            "period_start_value": period_start_value,
            "period_end_value": period_end_value,
            "period_net_cashflow": period_net_cashflow,
            "period_pnl_abs": period_pnl_abs,
            "period_twr_pct": twr_by_date.get(max_date),
            "has_full_history_from_zero": has_full_history_from_zero,
            "positions_value_sum": positions_value_sum,
            "reconciliation_gap_abs": reconciliation_gap_abs,
            "reconciliation_by_asset_type": reconciliation_rows,
            "snapshot_count": len(daily_csv_rows),
            "positions_count": len(positions_csv_rows),
            "operations_count": len(operations_csv_rows),
            "income_events_count": len(income_csv_rows),
        },
        "timeseries_daily": daily_csv_rows,
        "positions_current": positions_csv_rows,
        "operations": operations_csv_rows,
        "income_events": income_csv_rows,
        "asset_aliases": [
            {
                "logical_asset_id": row["asset_uid"],
                "asset_uid": row["asset_uid"],
                "instrument_uid": row["instrument_uid"],
                "figi": row["figi"],
                "ticker": row["ticker"],
                "name": row["name"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in asset_alias_rows
        ],
        "data_quality": {
            "unknown_operation_group_count": unknown_operation_groups,
            "mojibake_detected_count": mojibake_detected_count,
            "positions_missing_label_count": positions_missing_labels,
            "has_full_history_from_zero": has_full_history_from_zero,
            "alias_groups_count": alias_groups_count,
            "income_events_available": True,
        },
        "assumptions": [
            "В operations включены только исполненные операции после дедупликации по operation_id.",
            "Дневные cashflow-агрегаты привязаны к локальной дате Europe/Moscow.",
            "income_net в daily timeseries уже учитывает удержанный налог из income_events.",
            "operation_taxes_total не включает dividend/coupon tax, если тот же налог уже представлен в income_events.",
            "Архив считается period-first: lifetime return не вычисляется без полной истории с нуля.",
            "reconciliation_by_asset_type строится от snapshot totals по классам активов; нераскрытый остаток остаётся residual.",
        ],
    }

    return dataset, daily_csv_rows, positions_csv_rows, operations_csv_rows, income_csv_rows


def build_dataset_readme(dataset: dict) -> str:
    summary = dataset["summary"]
    meta = dataset["meta"]
    return (
        "# FinanceTracker AI Dataset\n\n"
        "Этот архив подготовлен командой `/dataset` для передачи ИИ-модели.\n\n"
        "## Контекст\n\n"
        f"- Счёт: {meta['account_name']}\n"
        f"- Таймзона агрегации: {meta['timezone']}\n"
        f"- Период: {meta['period_start']} .. {meta['period_end']}\n"
        f"- Базовая валюта: {meta['base_currency']}\n"
        f"- Сформировано: {meta['generated_at']}\n\n"
        "## Файлы\n\n"
        "- `dataset.json` — основной структурированный датасет для ИИ.\n"
        "- `daily_timeseries.csv` — дневной ряд стоимости и денежных потоков.\n"
        "- `positions_current.csv` — текущие позиции на последнем снапшоте.\n"
        "- `operations.csv` — исполненные операции после дедупликации.\n"
        "- `income_events.csv` — купоны и дивиденды в нормализованном виде.\n\n"
        "## Ключевые поля\n\n"
        f"- Current value: {decimal_to_str(summary['current_value'])} {meta['base_currency']}\n"
        f"- Period start value: {decimal_to_str(summary['period_start_value'])} {meta['base_currency']}\n"
        f"- Period end value: {decimal_to_str(summary['period_end_value'])} {meta['base_currency']}\n"
        f"- Period net cashflow: {decimal_to_str(summary['period_net_cashflow'])} {meta['base_currency']}\n"
        f"- Period pnl abs: {decimal_to_str(summary['period_pnl_abs'])} {meta['base_currency']}\n"
        f"- Period twr pct: {summary['period_twr_pct']}\n"
        f"- Positions value sum: {decimal_to_str(summary['positions_value_sum'])} {meta['base_currency']}\n"
        f"- Reconciliation gap abs: {decimal_to_str(summary['reconciliation_gap_abs'])} {meta['base_currency']}\n"
        f"- Full history from zero: {summary['has_full_history_from_zero']}\n\n"
        "## Правила интерпретации\n\n"
        "- Денежные значения в JSON сохраняются как строки, чтобы не терять точность.\n"
        "- `operation_group` нормализует сырые типы операций; налоги по операциям экспортируются как `income_tax`.\n"
        "- `logical_asset_id` строится из `asset_uid` и нужен для склейки бумаг при смене FIGI.\n"
        "- `income_net` в дневном ряду уже очищен от удержанного налога по income_events.\n"
        "- `taxes_total` дедуплицирован: dividend/coupon tax не суммируется второй раз из operations, если он уже попал в income_events.\n"
        "- Если `has_full_history_from_zero=false`, архив нельзя трактовать как полную lifetime-историю портфеля.\n"
        "- Если `reconciliation_gap_abs` не равен нулю, смотрите `reconciliation_by_asset_type`: это residual между snapshot totals и суммой позиционных оценок.\n"
        "- Для подробного анализа сначала читайте `dataset.json`, затем CSV-файлы как табличную детализацию.\n"
    )


def create_dataset_archive() -> tuple[str, str]:
    with db_session() as session:
        dataset, daily_rows, positions_rows, operations_rows, income_rows = build_dataset_export(session)

    archive_name = f"fintracker_dataset_{dataset['meta']['period_end']}.zip"
    archive_tmp = tempfile.NamedTemporaryFile(prefix="fintracker_dataset_", suffix=".zip", delete=False)
    archive_path = archive_tmp.name
    archive_tmp.close()

    json_text = json.dumps(dataset, ensure_ascii=False, indent=2, default=json_default)
    readme_text = build_dataset_readme(dataset)

    daily_fields = [
        "date",
        "snapshot_at_utc",
        "portfolio_value",
        "expected_yield",
        "expected_yield_pct",
        "deposits",
        "withdrawals",
        "income_net",
        "commissions",
        "operation_taxes",
        "income_taxes",
        "net_cashflow",
        "day_pnl",
        "twr_pct",
    ]
    positions_fields = [
        "snapshot_date",
        "snapshot_at_utc",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "position_uid",
        "figi",
        "ticker",
        "name",
        "instrument_type",
        "quantity",
        "currency",
        "current_price",
        "current_nkd",
        "position_value",
        "expected_yield",
        "expected_yield_pct",
        "weight_pct",
        "value_source",
    ]
    operations_fields = [
        "operation_id",
        "date_utc",
        "local_date",
        "operation_type",
        "operation_group",
        "state",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "figi",
        "name",
        "amount",
        "currency",
        "price",
        "quantity",
        "commission",
        "yield_amount",
        "description",
        "description_has_mojibake",
        "source",
    ]
    income_fields = [
        "event_date",
        "event_type",
        "logical_asset_id",
        "asset_uid",
        "figi",
        "ticker",
        "instrument_name",
        "gross_amount",
        "tax_amount",
        "net_amount",
        "net_yield_pct",
        "notified",
    ]

    with tempfile.TemporaryDirectory(prefix="fintracker_dataset_") as temp_dir:
        dataset_json_path = os.path.join(temp_dir, "dataset.json")
        with open(dataset_json_path, "w", encoding="utf-8") as f:
            f.write(json_text)

        readme_path = os.path.join(temp_dir, "README_AI.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_text)

        write_csv_file(os.path.join(temp_dir, "daily_timeseries.csv"), daily_fields, daily_rows)
        write_csv_file(os.path.join(temp_dir, "positions_current.csv"), positions_fields, positions_rows)
        write_csv_file(os.path.join(temp_dir, "operations.csv"), operations_fields, operations_rows)
        write_csv_file(os.path.join(temp_dir, "income_events.csv"), income_fields, income_rows)

        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(dataset_json_path, arcname="dataset.json")
            archive.write(readme_path, arcname="README_AI.md")
            archive.write(os.path.join(temp_dir, "daily_timeseries.csv"), arcname="daily_timeseries.csv")
            archive.write(os.path.join(temp_dir, "positions_current.csv"), arcname="positions_current.csv")
            archive.write(os.path.join(temp_dir, "operations.csv"), arcname="operations.csv")
            archive.write(os.path.join(temp_dir, "income_events.csv"), arcname="income_events.csv")

    return archive_path, archive_name


def build_triggers_messages() -> list[str]:
    """
    Ежедневные триггеры:
    - новый максимум (реальное обновление исторического максимума)
    - годовой план выполнен
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    year = today.year

    messages: list[str] = []

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return messages

        snaps = get_latest_snapshots(session, account_id, limit=2)
        if not snaps:
            return messages

        last = snaps[0]
        last_value = float(last["total_value"])
        last_date = last["snapshot_date"]

        prev_value = None
        prev_date = None
        if len(snaps) >= 2:
            prev = snaps[1]
            prev_value = float(prev["total_value"])
            prev_date = prev["snapshot_date"]

        # Исторический максимум на все даты строго ДО текущей даты last_date.
        # Если его нет, значит это самый первый день — "новый максимум" не шлём, чтобы не спамить.
        max_before_last = get_max_value_before_date(session, account_id, last_date)

        # Для годового плана — сравниваем сумму пополнений до вчера и до сегодня
        year_start = datetime(year, 1, 1)
        today_start = datetime(year, today.month, today.day)
        tomorrow_start = today_start + timedelta(days=1)

        dep_prev = get_deposits_for_period(session, account_id, year_start, today_start)
        dep_now = get_deposits_for_period(session, account_id, year_start, tomorrow_start)

    # Новый максимум: текущая стоимость должна быть строго больше
    # исторического максимума до сегодняшнего дня.
    if max_before_last is not None and last_value > max_before_last:
        messages.append(
            "🎉 Новый максимум стоимости портфеля!\n\n"
            f"Текущая оценка: *{fmt_rub(last_value)}*\n"
            f"Предыдущий максимум: {fmt_rub(max_before_last)}."
        )

    # Годовой план выполнен (считаем по дню пересечения)
    if PLAN_ANNUAL_CONTRIB_RUB > 0:
        plan = PLAN_ANNUAL_CONTRIB_RUB
        if dep_prev < plan <= dep_now:
            messages.append(
                f"✅ За год внесено *{fmt_rub(dep_now)}* — годовой план "
                f"по пополнениям ({fmt_rub(plan)}) выполнен! 👏"
            )

    return messages


async def jobqueue_smoke_test_job(context: ContextTypes.DEFAULT_TYPE):
    sent = 0
    failed = 0
    now_local = datetime.now(TZ)
    text_msg = (
        "🧪 JobQueue smoke-test\n"
        f"Время (локальное): {now_local.strftime('%d.%m.%Y %H:%M:%S %Z')}\n"
        "Отправка из одноразового тестового джоба при старте."
    )

    for chat_id in TARGET_CHAT_IDS:
        try:
            await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
            logger.exception(
                "jobqueue_smoke_test_failed",
                "JobQueue smoke-test failed.",
                {"chat_id": chat_id},
            )

    logger.info(
        "jobqueue_smoke_test_completed",
        "JobQueue smoke-test completed.",
        {
            "sent": sent,
            "failed": failed,
            "target_chat_ids": sorted(TARGET_CHAT_IDS),
        },
    )


async def polling_watchdog_job(context: ContextTypes.DEFAULT_TYPE):
    global POLLING_BACKLOG_ACTIVE

    now_utc = datetime.now(timezone.utc)
    reference_dt = LAST_UPDATE_RECEIVED_AT_UTC or BOT_PROCESS_STARTED_AT_UTC
    stall_duration_seconds = int((now_utc - reference_dt).total_seconds())

    try:
        webhook_info = await context.bot.get_webhook_info()
        pending_update_count = int(webhook_info.pending_update_count or 0)
    except Exception:
        logger.exception(
            "bot_polling_watchdog_failed",
            "Polling watchdog failed to query Telegram webhook state.",
            {
                "stall_duration_seconds": stall_duration_seconds,
                "last_update_received_at": to_iso_datetime(LAST_UPDATE_RECEIVED_AT_UTC),
                "process_started_at": to_iso_datetime(BOT_PROCESS_STARTED_AT_UTC),
            },
        )
        return

    backlog_detected = is_polling_backlog_detected(
        pending_update_count=pending_update_count,
        last_update_received_at=LAST_UPDATE_RECEIVED_AT_UTC,
        process_started_at=BOT_PROCESS_STARTED_AT_UTC,
        now_utc=now_utc,
    )
    ctx = {
        "pending_update_count": pending_update_count,
        "stall_duration_seconds": stall_duration_seconds,
        "pending_threshold": POLLING_BACKLOG_PENDING_THRESHOLD,
        "stall_threshold_seconds": POLLING_BACKLOG_STALL_THRESHOLD_SECONDS,
        "last_update_received_at": to_iso_datetime(LAST_UPDATE_RECEIVED_AT_UTC),
        "process_started_at": to_iso_datetime(BOT_PROCESS_STARTED_AT_UTC),
    }

    if backlog_detected and not POLLING_BACKLOG_ACTIVE:
        POLLING_BACKLOG_ACTIVE = True
        logger.error(
            "bot_polling_backlog_detected",
            "Telegram polling appears stalled: updates are accumulating.",
            ctx,
        )
        return

    if not backlog_detected and POLLING_BACKLOG_ACTIVE:
        POLLING_BACKLOG_ACTIVE = False
        logger.info(
            "bot_polling_backlog_cleared",
            "Telegram polling backlog cleared.",
            ctx,
        )


# =============== HANDLERS =================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/start")
    if not is_authorized(update):
        return
    text = (
        "Привет! Я слежу за вашим портфелем «Семейный капитал».\n\n"
        "Доступные команды можно посмотреть в /help."
    )
    logger.info(
        "bot_reply_text_started",
        "Sending reply_text response.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )
    await update.message.reply_text(text)
    logger.info(
        "bot_reply_text_succeeded",
        "reply_text response sent.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/help")
    if not is_authorized(update):
        return

    text = build_help_text()
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/today")
    if not is_authorized(update):
        return
    text = build_today_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/week")
    if not is_authorized(update):
        return
    text = build_week_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/month")
    if not is_authorized(update):
        return
    text = build_month_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/year")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) > 1:
        await update.message.reply_text("Формат: /year или /year YYYY")
        return

    year: int | None = None
    if len(args) == 1:
        try:
            parsed_year = int(args[0])
            if parsed_year < 1900 or parsed_year > 2100:
                raise ValueError
            year = parsed_year
        except ValueError:
            await update.message.reply_text("Формат: /year или /year YYYY")
            return

    try:
        summary_text, diff_text, label = build_year_summary(year)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    temp_chart = tempfile.NamedTemporaryFile(prefix=f"year_{chart_year}_", suffix=".png", delete=False)
    chart_path = temp_chart.name
    temp_chart.close()
    try:
        chart = build_year_chart(
            chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(chart_path):
            os.remove(chart_path)
        await update.message.reply_text(str(exc))
        return
    if chart:
        try:
            with open(chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(chart):
                os.remove(chart)
    else:
        await update.message.reply_text(f"Недостаточно данных для графика за {label}.")

    temp_delta_chart = tempfile.NamedTemporaryFile(prefix=f"year_delta_{chart_year}_", suffix=".png", delete=False)
    delta_chart_path = temp_delta_chart.name
    temp_delta_chart.close()
    try:
        delta_chart = build_year_monthly_delta_chart(
            delta_chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(delta_chart_path):
            os.remove(delta_chart_path)
        await update.message.reply_text(str(exc))
        return
    if delta_chart:
        try:
            with open(delta_chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(delta_chart):
                os.remove(delta_chart)

    await safe_send_message(context.bot, update.effective_chat.id, diff_text, parse_mode="Markdown")


async def cmd_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/dataset")
    if not is_authorized(update):
        return

    if context.args:
        await update.message.reply_text("Формат: /dataset")
        return

    try:
        archive_path, archive_name = create_dataset_archive()
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    try:
        with open(archive_path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=archive_name),
                caption="Архив для AI-анализа: JSON, CSV и README с контекстом.",
            )
    finally:
        if os.path.exists(archive_path):
            os.remove(archive_path)


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/structure")
    if not is_authorized(update):
        return
    text = build_structure_text()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/history")
    if not is_authorized(update):
        return

    path = "/tmp/history.png"
    try:
        p = build_history_chart(path)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    if not p:
        await update.message.reply_text(
            "Недостаточно данных для построения графика."
        )
        return

    with open(p, "rb") as f:
        # Caption убран по требованию
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_twr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/twr")
    if not is_authorized(update):
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return

        data = compute_twr_timeseries(session, account_id)
        xirr_value, projected_value, projection_date = compute_portfolio_xirr_and_run_rate(
            session,
            account_id,
        )

    if not data:
        await update.message.reply_text("Недостаточно данных")
        return

    dates, values, twr = data
    last_date = dates[-1]
    last_value = values[-1]
    last_twr_pct = twr[-1] * 100.0
    summary_text = render_twr_summary_text(
        last_date=last_date,
        last_value=last_value,
        last_twr_pct=last_twr_pct,
        xirr_value=xirr_value,
        projected_value=projected_value,
        projection_date=projection_date,
    )
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    path = "/tmp/twr.png"
    render_twr_chart(path, dates, values, twr)

    with open(path, "rb") as f:
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/targets")
    if not is_authorized(update):
        return

    args = context.args or []
    if not args:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if account_id is None:
                await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
                return
            text = build_targets_text_for_account(session, account_id)
        await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
        return

    if args[0].lower() != "set":
        await update.message.reply_text(TARGETS_USAGE_TEXT)
        return

    try:
        targets = parse_rebalance_targets_args(args[1:])
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        saved = replace_rebalance_targets(session, account_id, targets)
        if not saved:
            await update.message.reply_text(REBALANCE_FEATURE_UNAVAILABLE_TEXT)
            return

    with db_session() as session:
        text = build_targets_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/rebalance")
    if not is_authorized(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /rebalance")
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_rebalance_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_invest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/invest")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) != 1:
        await update.message.reply_text(INVEST_USAGE_TEXT)
        return

    try:
        amount = parse_decimal_input(args[0], allow_zero=False)
        if quantize_ruble_amount(amount) <= 0:
            raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    rounded_amount = quantize_ruble_amount(amount)
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_invest_text_for_account(
            session,
            account_id,
            rounded_amount,
            header=f"💸 Как распределить пополнение {fmt_decimal_rub(rounded_amount, precision=0)}",
        )
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


# ============ DAILY JOB (JOBQUEUE) ========


async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Авто-рассылки по расписанию (по TIMEZONE):
    - каждый день в DAILY_SUMMARY_HOUR:DAILY_SUMMARY_MINUTE (по TIMEZONE): проверка триггеров
      (новый максимум / годовой план)
    - каждую пятницу в DAILY_SUMMARY_HOUR:DAILY_SUMMARY_MINUTE (по TIMEZONE): недельный отчёт (/week)
    - в последний день месяца в DAILY_SUMMARY_HOUR:DAILY_SUMMARY_MINUTE (по TIMEZONE): месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = DAILY_JOB_SCHEDULE_LABEL

    logger.info(
        "daily_job_started",
        "Daily job started.",
        {
            "today": today.isoformat(),
            "scheduled_for": scheduled_for,
            "started_at": started_at.isoformat(),
            "is_month_end": is_month_end,
            "is_friday": is_friday,
        },
    )

    month_text = None
    week_text = None
    triggers: list[str] = []

    try:
        if is_month_end:
            month_text = build_month_summary()
    except Exception:
        logger.exception("daily_job_month_summary_failed", "Failed to build month summary.")

    try:
        if is_friday:
            week_text = build_week_summary()
    except Exception:
        logger.exception("daily_job_week_summary_failed", "Failed to build week summary.")

    try:
        triggers = build_triggers_messages()
    except Exception:
        logger.exception("daily_job_triggers_failed", "Failed to build trigger messages.")

    logger.info(
        "daily_job_messages_prepared",
        "Daily job prepared messages.",
        {
            "month_report_ready": bool(month_text),
            "week_report_ready": bool(week_text),
            "triggers_count": len(triggers),
        },
    )

    # Нечего отправлять — выходим тихо.
    if not month_text and not week_text and not triggers:
        logger.info(
            "daily_job_no_messages",
            "Daily job had no messages to send.",
            {"today": today.isoformat()},
        )
        return

    sent_total = 0
    failed_total = 0

    for chat_id in TARGET_CHAT_IDS:
        # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
        if is_month_end and month_text:
            try:
                await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job month report.",
                    {"chat_id": chat_id, "message_type": "month_report"},
                )

        if is_friday and week_text:
            try:
                await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job week report.",
                    {"chat_id": chat_id, "message_type": "week_report"},
                )

        for msg in triggers:
            try:
                await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
                sent_total += 1
                logger.info(
                    "daily_job_message_sent",
                    "Daily job message sent.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )
            except Exception:
                failed_total += 1
                logger.exception(
                    "daily_job_message_send_failed",
                    "Failed to send daily job trigger message.",
                    {"chat_id": chat_id, "message_type": "trigger"},
                )

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    logger.info(
        "daily_job_completed",
        "Daily job completed.",
        {
            "today": today.isoformat(),
            "duration_ms": duration_ms,
            "sent_total": sent_total,
            "failed_total": failed_total,
        },
    )


async def check_income_events(context: ContextTypes.DEFAULT_TYPE):
    rows: list[dict] = []
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return

        rows = (
            session.execute(
                text(
                    """
                    SELECT
                        ie.id,
                        ie.figi,
                        ie.event_type,
                        ie.net_amount,
                        ie.net_yield_pct,
                        COALESCE(i.name, i.ticker, ie.figi) AS instrument_name
                    FROM income_events ie
                    LEFT JOIN instruments i ON i.figi = ie.figi
                    WHERE ie.account_id = :account_id
                      AND ie.notified = false
                    ORDER BY ie.created_at ASC
                    """
                ),
                {"account_id": account_id},
            )
            .mappings()
            .all()
        )

    for row in rows:
        event_type = row["event_type"]
        icon = "💸" if event_type == "coupon" else "💰"
        action_line = "Купон зачислен" if event_type == "coupon" else "Дивиденды зачислены"
        net_amount = float(row["net_amount"])
        net_yield_pct = float(row["net_yield_pct"])
        instrument_name = row["instrument_name"]

        text_msg = (
            f"{icon} {instrument_name}\n"
            f"{action_line}\n"
            f"{fmt_signed_amount(net_amount)} ₽ ({fmt_plain_pct(net_yield_pct)} %)"
        )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "income_event_notification_sent",
                    "Income event notification sent.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "income_event_notification_failed",
                    "Failed to send income event notification.",
                    {
                        "income_event_id": row["id"],
                        "chat_id": chat_id,
                        "event_type": event_type,
                        "figi": row["figi"],
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            session.execute(
                text("UPDATE income_events SET notified = true WHERE id = :id"),
                {"id": row["id"]},
            )
            session.commit()


async def check_invest_notifications(context: ContextTypes.DEFAULT_TYPE):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return
        rows = get_pending_invest_notifications(session, account_id)

    if rows is None:
        return

    for row in rows:
        amount = normalize_decimal(row["amount"])
        with db_session() as session:
            text_msg = build_invest_text_for_account(
                session,
                account_id,
                amount,
                header=f"💸 Получено пополнение: {fmt_decimal_rub(amount, precision=0)}",
            )

        sent_ok = True
        for chat_id in TARGET_CHAT_IDS:
            try:
                await safe_send_message(context.bot, chat_id, text_msg, parse_mode="Markdown")
                logger.info(
                    "invest_notification_sent",
                    "Invest notification sent.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "invest_notification_failed",
                    "Failed to send invest notification.",
                    {
                        "operation_id": row["operation_id"],
                        "chat_id": chat_id,
                        "amount": decimal_to_str(amount),
                    },
                )

        if not sent_ok:
            continue

        with db_session() as session:
            marked = mark_invest_notification_sent(
                session,
                account_id=account_id,
                operation_id=row["operation_id"],
                operation_date=row["date"],
                amount=amount,
            )
        if not marked:
            return


async def on_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = getattr(context, "error", None)
    update_id = getattr(update, "update_id", None)
    user_id = getattr(getattr(update, "effective_user", None), "id", None)
    chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    ctx = {
        "update_id": update_id,
        "user_id": user_id,
        "chat_id": chat_id,
        "error_type": type(err).__name__ if err is not None else None,
        "error_message": str(err) if err is not None else None,
    }
    if err is not None:
        logger.raw_logger.error(
            "Unhandled Telegram application error.",
            extra={"event": "bot_application_error", "ctx": ctx},
            exc_info=(type(err), err, err.__traceback__),
        )
        return
    logger.error(
        "bot_application_error",
        "Unhandled Telegram application error without exception object.",
        ctx,
    )


def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "missing_telegram_bot_token",
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную.",
        )
        return 1

    app = build_bot_application()

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
    app.add_error_handler(on_application_error)

    logger.info(
        "bot_telegram_transport_configured",
        "Configured Telegram transport for polling and bot API requests.",
        {
            "proxy_enabled": BOT_PROXY_ENABLED,
            "proxy_url": BOT_PROXY_ENDPOINT if BOT_PROXY_ENABLED else None,
            "request_pool_size": TELEGRAM_REQUEST_CONNECTION_POOL_SIZE,
            "request_pool_timeout_seconds": TELEGRAM_REQUEST_POOL_TIMEOUT_SECONDS,
            "get_updates_pool_size": TELEGRAM_GET_UPDATES_CONNECTION_POOL_SIZE,
            "get_updates_pool_timeout_seconds": TELEGRAM_GET_UPDATES_POOL_TIMEOUT_SECONDS,
            "get_updates_timeout_seconds": TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS,
            "get_updates_read_timeout_seconds": TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS,
        },
    )

    # Ежедневный джоб
    job_time = build_daily_job_time()
    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    app.job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    app.job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")
    app.job_queue.run_repeating(check_invest_notifications, interval=60, first=15, name="invest_notifier")
    app.job_queue.run_repeating(
        polling_watchdog_job,
        interval=POLLING_WATCHDOG_INTERVAL_SECONDS,
        first=POLLING_WATCHDOG_INTERVAL_SECONDS,
        name="polling_watchdog",
    )
    logger.info(
        "bot_jobqueue_jobs_registered",
        "JobQueue jobs registered.",
        {
            "daily_job_schedule": DAILY_JOB_SCHEDULE_LABEL,
            "schedule_timezone": TZ_NAME,
            "target_chat_ids": sorted(TARGET_CHAT_IDS),
            "income_events_interval_seconds": 60,
            "invest_interval_seconds": 60,
            "polling_watchdog_interval_seconds": POLLING_WATCHDOG_INTERVAL_SECONDS,
        },
    )

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
            "daily_job_schedule": DAILY_JOB_SCHEDULE_LABEL,
            "schedule_timezone": TZ_NAME,
        },
    )
    app.run_polling(
        timeout=TELEGRAM_GET_UPDATES_TIMEOUT_SECONDS,
        poll_interval=TELEGRAM_POLL_INTERVAL_SECONDS,
    )
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
