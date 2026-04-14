from __future__ import annotations

import csv
import os
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from telegram import InputFile, Update

from common.logging_setup import configure_logging, get_logger

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

# Таймзона для отображения дат в тексте
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

# Таймзона хоста остаётся только для служебной диагностики.
HOST_TZ = datetime.now().astimezone().tzinfo

# Одноразовый тест JobQueue при старте (для валидации отправки).
JOBQUEUE_SMOKE_TEST_ON_START = (
    os.getenv("JOBQUEUE_SMOKE_TEST_ON_START", "false").strip().lower() in {"1", "true", "yes", "on"}
)
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS = int(os.getenv("JOBQUEUE_SMOKE_TEST_DELAY_SECONDS", "20"))

BOT_PROXY_ENABLED = (
    os.getenv("BOT_PROXY_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
BOT_PROXY_ENDPOINT = os.getenv("BOT_PROXY_ENDPOINT", "socks5h://xray-client:1080").strip()

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
POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT = 2
POLLING_SELF_HEAL_EXIT_CODE = 75

BOT_PROCESS_STARTED_AT_UTC = datetime.now(timezone.utc)
LAST_UPDATE_RECEIVED_AT_UTC: datetime | None = None

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

# Structured logging configuration
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
REPORTING_ACCOUNT_UNAVAILABLE_TEXT = (
    "Не удалось определить активный счёт для отчёта. "
    "Укажите корректный TINKOFF_ACCOUNT_ID или дождитесь первого снапшота."
)


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
        "httpx_kwargs": {"trust_env": False},
    }
    if proxy_url is not None:
        kwargs["proxy"] = proxy_url
    return kwargs


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


def next_polling_backlog_detection_streak(
    *,
    backlog_detected: bool,
    current_streak: int,
) -> int:
    if not backlog_detected:
        return 0
    return current_streak + 1


def should_trigger_polling_self_heal(
    *,
    backlog_detected: bool,
    detection_streak: int,
    recovery_confirmation_count: int = POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT,
) -> bool:
    return backlog_detected and detection_streak >= recovery_confirmation_count


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


async def safe_send_document(
    bot,
    chat_id: int,
    *,
    file_path: str,
    filename: str,
    caption: str | None = None,
):
    logger.info(
        "bot_send_document_started",
        "Sending Telegram document.",
        {
            "chat_id": chat_id,
            "filename": filename,
            "has_caption": caption is not None,
        },
    )
    try:
        with open(file_path, "rb") as file_obj:
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(file_obj, filename=filename),
                caption=caption,
            )
    except Exception:
        logger.exception(
            "bot_send_document_failed",
            "Telegram document send failed.",
            {
                "chat_id": chat_id,
                "filename": filename,
            },
        )
        raise

    logger.info(
        "bot_send_document_succeeded",
        "Telegram document sent.",
        {
            "chat_id": chat_id,
            "filename": filename,
        },
    )


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


def reset_update_tracking_state() -> None:
    global BOT_PROCESS_STARTED_AT_UTC
    global LAST_UPDATE_RECEIVED_AT_UTC

    BOT_PROCESS_STARTED_AT_UTC = datetime.now(timezone.utc)
    LAST_UPDATE_RECEIVED_AT_UTC = None


def get_process_started_at_utc() -> datetime:
    return BOT_PROCESS_STARTED_AT_UTC


def get_last_update_received_at_utc() -> datetime | None:
    return LAST_UPDATE_RECEIVED_AT_UTC


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


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    first_next = date(d.year, d.month + 1, 1)
    return first_next - timedelta(days=1)
