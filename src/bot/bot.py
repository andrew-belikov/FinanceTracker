"""
Telegram-бот для проекта iis_tracker.

Функции:
- Команды:
    /today      — сводка по портфелю "Семейный капитал" на сегодня
    /week       — сводка по текущей неделе
    /month      — отчёт по текущему месяцу
    /year       — отчёт за год (YTD или календарный)
    /structure  — текущая структура портфеля
    /history    — график стоимости портфеля и суммы пополнений
    /twr        — TWR (time-weighted return) и график по дням
    /help       — список команд

- Ежедневная задача (18:00 по времени хоста, через JobQueue):
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
import logging
import random
import tempfile
from contextlib import contextmanager
from decimal import Decimal
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from telegram import Update, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
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

# Таймзона для отображения дат в тексте
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
TZ = ZoneInfo(TZ_NAME)

# Таймзона хоста для расписания JobQueue
HOST_TZ = datetime.now().astimezone().tzinfo

# Одноразовый тест JobQueue при старте (для валидации отправки).
JOBQUEUE_SMOKE_TEST_ON_START = (
    os.getenv("JOBQUEUE_SMOKE_TEST_ON_START", "false").strip().lower() in {"1", "true", "yes", "on"}
)
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS = int(os.getenv("JOBQUEUE_SMOKE_TEST_DELAY_SECONDS", "20"))

# Годовой план пополнений
PLAN_ANNUAL_CONTRIB_RUB = float(os.getenv("PLAN_ANNUAL_CONTRIB_RUB", "400000"))

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

# ==========================================

# Structured logging configuration
from common.logging_setup import configure_logging, get_logger

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
INCOME_OPERATION_TYPES: tuple[str, ...] = (
    "OPERATION_TYPE_COUPON",
    "OPERATION_TYPE_DIVIDEND",
)
EXECUTED_OPERATION_STATE = "OPERATION_STATE_EXECUTED"

OPERATIONS_DEDUP_CTE = """
WITH operations_dedup AS (
    SELECT DISTINCT ON (COALESCE(operation_id, id::text))
        id,
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
    ORDER BY COALESCE(operation_id, id::text), id DESC
)
"""

YEAR_REPORT_TOP_N = 5


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
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
    except Exception:
        # Иногда ломается Markdown из-за динамических значений (тикеры с _ и т.п.)
        await bot.send_message(chat_id=chat_id, text=text)


# =============== HELPERS ==================


def is_authorized(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    return user.id in ALLOWED_USER_IDS


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


def get_latest_snapshots(session, limit: int = 2):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT :limit
        """
            ),
            {"limit": limit},
        )
        .mappings()
        .all()
    )
    return list(rows)


def get_latest_snapshot_date(session):
    return session.execute(
        text("SELECT MAX(snapshot_date) FROM portfolio_snapshots")
    ).scalar_one()


def get_latest_deposit_date(
    session,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    return session.execute(
        text(
            """
            SELECT MAX(date::date)
            FROM operations
            WHERE operation_type IN :operation_types
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {"operation_types": operation_types},
    ).scalar_one()


def get_total_deposits(
    session,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            """
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations
            WHERE operation_type IN :operation_types
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {"operation_types": operation_types},
    ).scalar_one()
    return float(row or 0)


def get_deposits_for_period(
    session,
    start_dt: datetime,
    end_dt: datetime,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            """
        SELECT COALESCE(SUM(amount), 0) AS s
        FROM operations
        WHERE date >= :start_dt AND date < :end_dt
          AND operation_type IN :operation_types
        """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "start_dt": start_dt,
            "end_dt": end_dt,
            "operation_types": operation_types,
        },
    ).scalar_one()
    return float(row or 0)


def _is_undefined_table_error(exc: Exception, table_name: str) -> bool:
    if not isinstance(exc, ProgrammingError):
        return False
    if getattr(exc.orig, "pgcode", None) == "42P01":
        return True
    return f'relation "{table_name}" does not exist' in str(exc).lower()


def get_income_for_period(db, start_date, end_date) -> tuple[Decimal, Decimal]:
    try:
        row = db.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
                    COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends
                FROM income_events
                WHERE event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings().one()
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            return Decimal("0"), Decimal("0")
        raise

    return Decimal(row["coupons"] or 0), Decimal(row["dividends"] or 0)


def get_commissions_for_period(db, start_date, end_date) -> Decimal:
    total = db.execute(
        text(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM operations
            WHERE date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": COMMISSION_OPERATION_TYPES,
        },
    ).scalar_one()
    return abs(Decimal(total or 0))


def get_taxes_for_period(db, start_date, end_date) -> Decimal:
    income_taxes = Decimal("0")
    try:
        income_taxes_row = db.execute(
            text(
                """
                SELECT COALESCE(SUM(tax_amount), 0) AS total
                FROM income_events
                WHERE event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).scalar_one()
        income_taxes = Decimal(income_taxes_row or 0)
    except Exception as exc:
        if not _is_undefined_table_error(exc, "income_events"):
            raise

    operation_taxes = db.execute(
        text(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0) AS total
            FROM operations
            WHERE date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
        },
    ).scalar_one()
    return income_taxes + Decimal(operation_taxes or 0)


def get_month_snapshots(session, year: int, month: int):
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
        WHERE snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"start": month_start, "end": next_month_start},
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
        WHERE snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"start": month_start, "end": next_month_start},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_period_snapshots(session, start_date: date, end_date_exclusive: date):
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
        WHERE snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"start": start_date},
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
        WHERE snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"start": start_date, "end": end_date_exclusive},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_latest_snapshot_with_id(session):
    row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            )
        )
        .mappings()
        .first()
    )
    return row


def get_positions_for_snapshot(session, snapshot_id: int):
    rows = (
        session.execute(
            text(
                """
        SELECT
            figi,
            COALESCE(ticker, '') AS ticker,
            COALESCE(name, '')   AS name,
            instrument_type,
            quantity,
            current_price,
            position_value,
            expected_yield,
            expected_yield_pct,
            weight_pct
        FROM portfolio_positions
        WHERE snapshot_id = :sid
        ORDER BY position_value DESC
        """
            ),
            {"sid": snapshot_id},
        )
        .mappings()
        .all()
    )
    return rows


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


def compute_positions_diff_grouped(session, from_dt: datetime, to_dt: datetime) -> tuple[list[str], str | None]:
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
            WHERE snapshot_at >= :from_dt
              AND snapshot_at < :to_dt
            ORDER BY snapshot_date ASC, snapshot_at ASC
            """
        ),
        {"from_dt": from_dt, "to_dt": to_dt},
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

        figi_changed = (
            key in start_figis_by_key
            and key in end_figis_by_key
            and start_figis_by_key.get(key, set()) != end_figis_by_key.get(key, set())
        )
        figi_suffix = " (сменился FIGI)" if figi_changed else ""

        if qty0 == 0 and qty1 > 0:
            grouped.append(("🆕 Новые", name, f"+ {name}: {_fmt_qty(0.0)} → {_fmt_qty(qty1)} шт{figi_suffix}"))
        elif qty0 > 0 and qty1 == 0:
            grouped.append(("✅ Закрыли", name, f"- {name}: {_fmt_qty(qty0)} → {_fmt_qty(0.0)} шт{figi_suffix}"))
        elif qty1 > qty0:
            grouped.append(("📈 Докупили", name, f"↑ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт{figi_suffix}"))
        elif qty1 < qty0 and qty1 > 0:
            grouped.append(("📉 Продали часть", name, f"↓ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт{figi_suffix}"))

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


def get_portfolio_timeseries(session):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, total_value
        FROM portfolio_snapshots
        ORDER BY snapshot_date ASC
        """
            )
        )
        .mappings()
        .all()
    )
    return rows


def get_deposits_by_date(
    session,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    rows = (
        session.execute(
            text(
                """
        SELECT date::date AS d, SUM(amount) AS s
        FROM operations
        WHERE operation_type IN :operation_types
        GROUP BY date::date
        ORDER BY d ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {"operation_types": operation_types},
        )
        .mappings()
        .all()
    )
    return rows


def get_year_financials_from_operations(session, start_dt: datetime, end_dt: datetime) -> dict[str, Decimal]:
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
            WHERE date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
            """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
        ),
        {
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


def compute_realized_by_asset(session, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
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
                WHERE od.date >= :start_dt
                  AND od.date < :end_dt
                  AND od.state = :executed_state
                  AND od.operation_type = 'OPERATION_TYPE_SELL'
                  AND od.figi IS NOT NULL
                GROUP BY od.figi
                ORDER BY amount DESC
                """
            ),
            {
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


def compute_income_by_asset_net(session, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
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
                WHERE od.date >= :start_dt
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


def get_unrealized_at_period_end(session, to_dt: datetime) -> Decimal:
    to_date = to_dt.date() - timedelta(days=1)
    snap = (
        session.execute(
            text(
                """
                SELECT id, expected_yield
                FROM portfolio_snapshots
                WHERE snapshot_date <= :to_date
                ORDER BY snapshot_date DESC, snapshot_at DESC
                LIMIT 1
                """
            ),
            {'to_date': to_date},
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
                WHERE date >= :start_dt
                  AND date < :end_dt
                  AND operation_type IN :operation_types
                  AND state = :executed_state
                GROUP BY date::date
                ORDER BY d ASC
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
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
                    WHERE snapshot_date >= :from_date
                      AND snapshot_date < :to_date
                ) month_snaps
                WHERE rn = 1
                ORDER BY month_start ASC
                """
            ),
            {
                "from_date": from_dt.date(),
                "to_date": to_dt.date(),
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_deposits(session, from_dt: datetime, to_dt: datetime):
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    date_trunc('month', date)::date AS month_start,
                    SUM(amount) AS amount
                FROM operations_dedup
                WHERE date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type = 'OPERATION_TYPE_INPUT'
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ),
            {
                "from_dt": from_dt,
                "to_dt": to_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows

def get_portfolio_timeseries_agg_by_date(session):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, SUM(total_value) AS total_value
        FROM portfolio_snapshots
        GROUP BY snapshot_date
        ORDER BY snapshot_date ASC
        """
            )
        )
        .mappings()
        .all()
    )
    return rows


def get_deposits_raw(
    session,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    rows = (
        session.execute(
            text(
                """
        SELECT date, amount
        FROM operations
        WHERE operation_type IN :operation_types
        ORDER BY date ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {"operation_types": operation_types},
        )
        .mappings()
        .all()
    )
    return rows


def get_max_value_before_date(session, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE snapshot_date < :d
        """
        ),
        {"d": d},
    ).scalar_one()
    return float(row) if row is not None else None


def get_max_value_to_date(session, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE snapshot_date <= :d
        """
        ),
        {"d": d},
    ).scalar_one()
    return float(row) if row is not None else None


# ========= BUSINESS CALCULATIONS ==========


def build_today_summary() -> str:
    """
    Формирует текст сводки "на сегодня" используя шаблоны из today_templates.
    """
    now_local = datetime.now(TZ)
    day_start = datetime.combine(now_local.date(), time.min)
    day_end_exclusive = day_start + timedelta(days=1)
    day_end = day_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        snaps = get_latest_snapshots(session, limit=2)
        total_deposits = get_total_deposits(session)
        coupons, dividends = get_income_for_period(session, day_start, day_end)
        commissions = get_commissions_for_period(session, day_start, day_end)
        taxes = get_taxes_for_period(session, day_start, day_end)

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
    if last_value is not None and prev_value is not None and prev_value != 0:
        delta_abs = last_value - prev_value
        delta_pct = delta_abs / prev_value * 100.0

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
        # 1. Определяем даты текущей рабочей недели (понедельник–пятница)
        latest_snap = get_latest_snapshot_with_id(session)
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

        # 3. Изменение за неделю
        # Ищем снапшот до начала недели, чтобы посчитать дельту
        # Если снапшота ровно в start_date нет, берем ближайший предыдущий
        # Если портфель моложе недели, берем самый первый
        start_val_row = session.execute(
            text(
                """
            SELECT total_value
            FROM portfolio_snapshots
            WHERE snapshot_date < :d
            ORDER BY snapshot_date DESC, snapshot_at DESC
            LIMIT 1
            """
            ),
            {"d": week_start_date},
        ).scalar()

        start_value = float(start_val_row) if start_val_row is not None else 0.0

        week_delta_abs = None
        week_delta_pct = None
        
        # Если start_value == 0 или None, значит до начала недели данных не было
        # Но если портфель создан внутри недели, можно считать start_value = 0?
        # Будем считать дельту только если есть старое значение.
        if start_val_row is not None and start_value != 0:
            week_delta_abs = current_value - start_value
            week_delta_pct = week_delta_abs / start_value * 100.0
        elif start_val_row is None:
            # Портфель появился на этой неделе
            week_delta_abs = current_value
            week_delta_pct = 0.0 # Или None, как удобнее

        # 4. Пополнения/доходы/расходы за текущую рабочую неделю
        dep_week = get_deposits_for_period(session, week_start, week_end_exclusive)
        coupons, dividends = get_income_for_period(session, week_start, week_end)
        commissions = get_commissions_for_period(session, week_start, week_end)
        taxes = get_taxes_for_period(session, week_start, week_end)

        # 5. Прогресс по годовому плану
        year_start = datetime(week_end_date.year, 1, 1)
        dep_year = get_deposits_for_period(session, year_start, week_end_exclusive)
        
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
        # Пополнения за месяц
        dep_month = get_deposits_for_period(
            session,
            start_dt=month_start_dt,
            end_dt=month_end_exclusive,
        )

        coupons, dividends = get_income_for_period(session, month_start_dt, month_end_dt)
        commissions = get_commissions_for_period(session, month_start_dt, month_end_dt)
        taxes = get_taxes_for_period(session, month_start_dt, month_end_dt)

        # Пополнения за год
        dep_year = get_deposits_for_period(
            session,
            start_dt=datetime(year, 1, 1),
            end_dt=month_end_exclusive,
        )

        # Снапшоты для изменения стоимости за месяц
        start_snap, end_snap = get_month_snapshots(session, year, month)

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
        if start_val != 0:
            delta_abs = end_val - start_val
            delta_pct = delta_abs / start_val * 100.0

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


def build_structure_text() -> str:
    """
    Структура портфеля по последнему снапшоту, с разбивкой по типам:

    - сводка по типам (тип / сумма / доля / P&L)
    - далее блоки по типам (ETF, акции, валюта и т.д.), отсортированные по сумме
    - внутри блока — бумаги по убыванию суммы
    """
    with db_session() as session:
        snap = get_latest_snapshot_with_id(session)
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
        ts = get_portfolio_timeseries(session)
        deps = get_deposits_by_date(session)

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

    fig, ax = plt.subplots(figsize=(10, 4))

    ax.plot(week_dates, values, label="Стоимость портфеля")
    if cum_deps:
        ax.plot(week_dates, cum_deps, linestyle="--", label="Сумма пополнений")

    ax.set_title(ACCOUNT_FRIENDLY_NAME)
    ax.set_ylabel("₽")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return path


def build_year_chart(path: str, year: int, end_date_exclusive: date) -> str | None:
    year_start = date(year, 1, 1)
    period_start_dt = datetime.combine(year_start, time.min)
    period_end_dt_exclusive = datetime.combine(end_date_exclusive, time.min)
    is_ytd = end_date_exclusive.year == year and end_date_exclusive <= (datetime.now(TZ).date() + timedelta(days=1))

    with db_session() as session:
        portfolio_rows = get_monthly_portfolio_values(session, period_start_dt, period_end_dt_exclusive, is_ytd)
        deposits_rows = get_monthly_deposits(session, period_start_dt, period_end_dt_exclusive)

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
    labels = [m.strftime("%Y-%m") for m in months]

    fig, ax = plt.subplots(figsize=(10, 4))
    x = list(range(len(months)))
    ax.bar(x, portfolio_values, width=0.8, label="Стоимость портфеля")
    ax.bar(x, deposits_values, width=0.45, label="Пополнения")

    ax.set_title(f"{year}: стоимость портфеля и пополнения")
    ax.set_ylabel("₽")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
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
        year_financials = get_year_financials_from_operations(
            session,
            period_start_dt,
            period_end_dt_exclusive,
        )

        start_snap, end_snap = get_period_snapshots(session, period_start, period_end_dt_exclusive.date())
        diff_lines, diff_error = compute_positions_diff_grouped(session, period_start_dt, period_end_dt_exclusive)
        realized_by_asset, realized_total = compute_realized_by_asset(
            session,
            period_start_dt,
            period_end_dt_exclusive,
        )
        income_by_asset_net, income_total_net = compute_income_by_asset_net(
            session,
            period_start_dt,
            period_end_dt_exclusive,
        )
        unrealized = get_unrealized_at_period_end(session, period_end_dt_exclusive)

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
        f"Пополнения: *{fmt_rub(dep_year)}*",
        f"Прогресс годового плана: {plan_pct:.1f} % ({fmt_rub(dep_year)} / {fmt_rub(plan)})",
        "",
        f"Нереализовано на конец периода: {fmt_decimal_rub(unrealized)}",
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


def compute_twr_timeseries(session):
    ts = get_portfolio_timeseries_agg_by_date(session)
    if len(ts) < 2:
        return None

    deps = get_deposits_raw(session)
    dep_by_day: dict[date, float] = {}
    for row in deps:
        dt = row.get("date")
        if dt is None:
            continue
        # В БД date хранится как naive UTC
        dt_utc = dt.replace(tzinfo=timezone.utc)
        d_local = dt_utc.astimezone(TZ).date()
        amt = float(row.get("amount") or 0)
        dep_by_day[d_local] = dep_by_day.get(d_local, 0.0) + amt

    dates: list[date] = []
    values: list[float | None] = []
    for row in ts:
        dates.append(row["snapshot_date"])
        v = row["total_value"]
        values.append(float(v) if v is not None else None)

    M = 1.0
    twr: list[float] = [0.0]  # на первой точке TWR = 0
    for i in range(1, len(dates)):
        V_prev = values[i - 1]
        V = values[i]
        CF = dep_by_day.get(dates[i], 0.0)

        if V_prev in (None, 0) or V is None:
            # Не пересчитываем, но точку рисуем (последнее известное значение)
            twr.append(M - 1.0)
            continue

        r = (V - CF) / V_prev - 1.0
        M *= (1.0 + r)
        twr.append(M - 1.0)

    return dates, values, twr


def render_twr_chart(path: str, dates: list[date], values: list[float | None], twr: list[float]) -> str:
    twr_pct = [x * 100.0 for x in twr]

    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax2 = ax1.twinx()

    values_plot = [v if v is not None else float("nan") for v in values]

    ax1.plot(dates, values_plot, label="Стоимость портфеля")
    ax2.plot(dates, twr_pct, linestyle="--", label="TWR")

    ax1.set_title(ACCOUNT_FRIENDLY_NAME + " — TWR")
    ax1.set_ylabel("₽")
    ax2.set_ylabel("TWR, %")
    ax1.grid(True, alpha=0.3)

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="best")

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return path


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
        snaps = get_latest_snapshots(session, limit=2)
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
        max_before_last = get_max_value_before_date(session, last_date)

        # Для годового плана — сравниваем сумму пополнений до вчера и до сегодня
        year_start = datetime(year, 1, 1)
        today_start = datetime(year, today.month, today.day)
        tomorrow_start = today_start + timedelta(days=1)

        dep_prev = get_deposits_for_period(session, year_start, today_start)
        dep_now = get_deposits_for_period(session, year_start, tomorrow_start)

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
                "JobQueue smoke-test failed",
                extra={"ctx": {"chat_id": chat_id}},
            )

    logger.info(
        "JobQueue smoke-test completed",
        extra={
            "ctx": {
                "sent": sent,
                "failed": failed,
                "target_chat_ids": sorted(TARGET_CHAT_IDS),
            }
        },
    )


# =============== HANDLERS =================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = (
        "Привет! Я слежу за вашим портфелем «Семейный капитал».\n\n"
        "Доступные команды можно посмотреть в /help."
    )
    await update.message.reply_text(text)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return

    text = (
        "Доступные команды:\n\n"
        "/today — сводка по портфелю на сегодня\n"
        "/week — сводка по текущей неделе\n"
        "/month — отчёт по текущему месяцу\n"
        "/year [YYYY] — отчёт за год (без аргумента: текущий год YTD)\n"
        "/structure — текущая структура портфеля по позициям\n"
        "/history — график стоимости портфеля и суммы пополнений\n"
        "/twr — TWR (time-weighted return) и график по дням\n"
        "/help — эта подсказка\n\n"
        "Автоматически:\n"
        "• каждый день в 18:00 (по времени хоста) — проверка триггеров (максимум, годовой план)\n"
        "• по пятницам в 18:00 (по времени хоста) — недельный отчёт\n"
        "• в последний день месяца в 18:00 (по времени хоста) — дополнительный отчёт за месяц."
    )
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_today_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_week_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_month_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    summary_text, diff_text, label = build_year_summary(year)
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    temp_chart = tempfile.NamedTemporaryFile(prefix=f"year_{chart_year}_", suffix=".png", delete=False)
    chart_path = temp_chart.name
    temp_chart.close()
    chart = build_year_chart(
        chart_path,
        year=chart_year,
        end_date_exclusive=period_end_dt_exclusive.date(),
    )
    if chart:
        try:
            with open(chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(chart):
                os.remove(chart)
    else:
        await update.message.reply_text(f"Недостаточно данных для графика за {label}.")

    await safe_send_message(context.bot, update.effective_chat.id, diff_text, parse_mode="Markdown")


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_structure_text()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return

    path = "/tmp/history.png"
    p = build_history_chart(path)
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
    if not is_authorized(update):
        return

    with db_session() as session:
        data = compute_twr_timeseries(session)

    if not data:
        await update.message.reply_text("Недостаточно данных")
        return

    dates, values, twr = data
    last_date = dates[-1]
    last_value = values[-1]
    last_twr_pct = twr[-1] * 100.0

    await update.message.reply_text(
        f"TWR на {last_date.isoformat()}: {fmt_pct(last_twr_pct, precision=2)}\n"
        f"Стоимость портфеля: {fmt_rub(last_value)}"
    )

    path = "/tmp/twr.png"
    render_twr_chart(path, dates, values, twr)

    with open(path, "rb") as f:
        await update.message.reply_photo(
            photo=InputFile(f)
        )


# ============ DAILY JOB (JOBQUEUE) ========


async def daily_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Авто-рассылки по расписанию (по времени хоста):
    - каждый день в 18:00 (по времени хоста): проверка триггеров (новый максимум / годовой план)
    - каждую пятницу в 18:00 (по времени хоста): недельный отчёт (/week)
    - в последний день месяца в 18:00 (по времени хоста): месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4
    started_at = datetime.now(TZ)
    started_monotonic = datetime.now(timezone.utc)
    scheduled_for = f"18:00 {HOST_TZ}"

    logger.info(
        "Daily job started",
        extra={
            "ctx": {
                "today": today.isoformat(),
                "scheduled_for": scheduled_for,
                "started_at": started_at.isoformat(),
                "is_month_end": is_month_end,
                "is_friday": is_friday,
            }
        },
    )

    month_text = None
    week_text = None
    triggers: list[str] = []

    try:
        if is_month_end:
            month_text = build_month_summary()
    except Exception as e:
        logger.exception("Failed to build month summary: %s", e)

    try:
        if is_friday:
            week_text = build_week_summary()
    except Exception as e:
        logger.exception("Failed to build week summary: %s", e)

    try:
        triggers = build_triggers_messages()
    except Exception as e:
        logger.exception("Failed to build triggers: %s", e)

    logger.info(
        "Daily job prepared messages",
        extra={
            "ctx": {
                "month_report_ready": bool(month_text),
                "week_report_ready": bool(week_text),
                "triggers_count": len(triggers),
            }
        },
    )

    # Нечего отправлять — выходим тихо.
    if not month_text and not week_text and not triggers:
        logger.info("No messages to send for %s", today.isoformat())
        return

    sent_total = 0
    failed_total = 0

    for chat_id in TARGET_CHAT_IDS:
        # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
        if is_month_end and month_text:
            try:
                await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
                sent_total += 1
                logger.info("Message sent", extra={"ctx": {"chat_id": chat_id, "message_type": "month_report", "status": "sent"}})
            except Exception as e:
                failed_total += 1
                logger.error("Error sending month report to chat %s: %s", chat_id, e)
                logger.exception("Message failed", extra={"ctx": {"chat_id": chat_id, "message_type": "month_report", "status": "failed"}})

        if is_friday and week_text:
            try:
                await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
                sent_total += 1
                logger.info("Message sent", extra={"ctx": {"chat_id": chat_id, "message_type": "week_report", "status": "sent"}})
            except Exception as e:
                failed_total += 1
                logger.error("Error sending week report to chat %s: %s", chat_id, e)
                logger.exception("Message failed", extra={"ctx": {"chat_id": chat_id, "message_type": "week_report", "status": "failed"}})

        for msg in triggers:
            try:
                await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
                sent_total += 1
                logger.info("Message sent", extra={"ctx": {"chat_id": chat_id, "message_type": "trigger", "status": "sent"}})
            except Exception as e:
                failed_total += 1
                logger.error("Error sending trigger to chat %s: %s", chat_id, e)
                logger.exception("Message failed", extra={"ctx": {"chat_id": chat_id, "message_type": "trigger", "status": "failed"}})

    duration_ms = int((datetime.now(timezone.utc) - started_monotonic).total_seconds() * 1000)
    logger.info(
        "Daily job completed",
        extra={
            "ctx": {
                "today": today.isoformat(),
                "duration_ms": duration_ms,
                "sent_total": sent_total,
                "failed_total": failed_total,
            }
        },
    )


async def check_income_events(context: ContextTypes.DEFAULT_TYPE):
    rows: list[dict] = []
    with db_session() as session:
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
                    WHERE ie.notified = false
                    ORDER BY ie.created_at ASC
                    """
                )
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
                    extra={
                        "ctx": {
                            "income_event_id": row["id"],
                            "chat_id": chat_id,
                            "event_type": event_type,
                            "figi": row["figi"],
                        }
                    },
                )
            except Exception:
                sent_ok = False
                logger.exception(
                    "income_event_notification_failed",
                    extra={
                        "ctx": {
                            "income_event_id": row["id"],
                            "chat_id": chat_id,
                            "event_type": event_type,
                            "figi": row["figi"],
                        }
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


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не задан. Передай его через env-переменную."
        )

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("month", cmd_month))
    app.add_handler(CommandHandler("year", cmd_year))
    app.add_handler(CommandHandler("structure", cmd_structure))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("twr", cmd_twr))

    # Ежедневный джоб
    job_time = time(
        hour=18,
        minute=0,
        tzinfo=HOST_TZ,
    )
    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    app.job_queue.run_daily(daily_job, time=job_time, name="daily_summary")
    app.job_queue.run_repeating(check_income_events, interval=60, first=10, name="income_events_notifier")

    if JOBQUEUE_SMOKE_TEST_ON_START:
        app.job_queue.run_once(
            jobqueue_smoke_test_job,
            when=JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
            name="jobqueue_smoke_test",
        )
        logger.info(
            "Scheduled JobQueue smoke-test",
            extra={
                "ctx": {
                    "delay_seconds": JOBQUEUE_SMOKE_TEST_DELAY_SECONDS,
                    "target_chat_ids": sorted(TARGET_CHAT_IDS),
                }
            },
        )

    logger.info(
        "Bot started. Daily job at 18:00 %s",
        HOST_TZ,
    )
    app.run_polling()


if __name__ == "__main__":
    main()
