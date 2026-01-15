"""
Telegram-бот для проекта iis_tracker.

Функции:
- Команды:
    /today      — сводка по портфелю "Семейный капитал" на сегодня
    /week       — сводка по текущей неделе
    /month      — отчёт по текущему месяцу
    /structure  — текущая структура портфеля
    /history    — график стоимости портфеля и суммы пополнений
    /help       — список команд

- Ежедневная задача (18:00 МСК, через JobQueue):
    * в последний день месяца — отчёт за месяц (/month)
    * триггеры:
        - новый максимум портфеля
        - просадка от максимума больше порога
        - годовой план по пополнениям выполнен (400k за год)
    * (ежедневная сводка /today автоматически НЕ отправляется)

Безопасность:
- ALLOWED_USER_IDS — белый список Telegram user_id.
- Все остальные пользователи игнорируются.
"""

import os
import logging
import random
from contextlib import contextmanager
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
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

# Таймзона
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
TZ = ZoneInfo(TZ_NAME)

# Время ежедневного джоба
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", "18"))
DAILY_SUMMARY_MINUTE = int(os.getenv("DAILY_SUMMARY_MINUTE", "0"))

# Годовой план пополнений
PLAN_ANNUAL_CONTRIB_RUB = float(os.getenv("PLAN_ANNUAL_CONTRIB_RUB", "400000"))

# Порог просадки от максимума (в процентах, например 5 = -5%)
DRAWDOWN_ALERT_PCT = float(os.getenv("DRAWDOWN_ALERT_PCT", "5.0"))

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

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("iis_tracker_bot")

engine = create_engine(DB_DSN, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


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


def fmt_pct(x: float | None, precision: int = 2) -> str:
    if x is None:
        return "—"
    fmt = f"{{:+.{precision}f}} %"
    return fmt.format(x)


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    first_next = date(d.year, d.month + 1, 1)
    return first_next - timedelta(days=1)


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


def get_total_deposits(session) -> float:
    row = session.execute(
        text("SELECT COALESCE(SUM(amount), 0) AS s FROM deposits")
    ).scalar_one()
    return float(row or 0)


def get_deposits_for_period(
    session,
    start_dt: datetime,
    end_dt: datetime,
) -> float:
    row = session.execute(
        text(
            """
        SELECT COALESCE(SUM(amount), 0) AS s
        FROM deposits
        WHERE date >= :start_dt AND date < :end_dt
        """
        ),
        {"start_dt": start_dt, "end_dt": end_dt},
    ).scalar_one()
    return float(row or 0)


def get_month_snapshots(session, year: int, month: int):
    month_start = date(year, month, 1)
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    # первый снапшот в месяце
    start_row = (
        session.execute(
            text(
                """
        SELECT snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date ASC, snapshot_at ASC
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
        SELECT snapshot_date, snapshot_at, total_value
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


def get_deposits_by_date(session):
    rows = (
        session.execute(
            text(
                """
        SELECT date::date AS d, SUM(amount) AS s
        FROM deposits
        GROUP BY date::date
        ORDER BY d ASC
        """
            )
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


def get_deposits_raw(session):
    rows = (
        session.execute(
            text(
                """
        SELECT date, amount
        FROM deposits
        ORDER BY date ASC
        """
            )
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
    with db_session() as session:
        snaps = get_latest_snapshots(session, limit=2)
        total_deposits = get_total_deposits(session)

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
    )

    return render_today_text(ctx)


def build_week_summary() -> str:
    """
    Формирует текст еженедельной сводки используя шаблоны из week_templates.
    """
    with db_session() as session:
        # 1. Определяем даты (неделя = последние 7 дней до последнего снапшота включительно)
        latest_snap = get_latest_snapshot_with_id(session)
        if not latest_snap:
            return "Пока нет ни одного снапшота портфеля."

        end_date = latest_snap["snapshot_date"]  # date
        start_date = end_date - timedelta(days=6)

        # Формируем week_label (например "10–16 ноября 2025")
        month_name = MONTHS_RU.get(end_date.month, str(end_date.month))
        week_label = f"{start_date.day}–{end_date.day} {month_name} {end_date.year}"

        # 2. Текущая стоимость
        current_value = float(latest_snap["total_value"]) if latest_snap["total_value"] is not None else 0.0

        # 3. Изменение за неделю
        # Ищем снапшот на дату <= start_date, чтобы посчитать дельту
        # Если снапшота ровно в start_date нет, берем ближайший предыдущий
        # Если портфель моложе недели, берем самый первый
        start_val_row = session.execute(
            text(
                """
            SELECT total_value
            FROM portfolio_snapshots
            WHERE snapshot_date <= :d
            ORDER BY snapshot_date DESC, snapshot_at DESC
            LIMIT 1
            """
            ),
            {"d": start_date},
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

        # 4. Пополнения за неделю (start_date включительно, end_date включительно)
        # get_deposits_for_period требует datetime, верхняя граница эксклюзивна
        t_start = datetime(start_date.year, start_date.month, start_date.day)
        t_end_date_inc = end_date + timedelta(days=1)
        t_end = datetime(t_end_date_inc.year, t_end_date_inc.month, t_end_date_inc.day)

        dep_week = get_deposits_for_period(session, t_start, t_end)

        # 5. Прогресс по годовому плану
        year_start = datetime(end_date.year, 1, 1)
        dep_year = get_deposits_for_period(session, year_start, t_end)
        
        plan = PLAN_ANNUAL_CONTRIB_RUB
        plan_pct = (dep_year / plan * 100.0) if plan > 0 else 0.0

    ctx = WeekContext(
        week_label=week_label,
        current_value=fmt_rub(current_value),
        week_delta_abs=fmt_rub(week_delta_abs) if week_delta_abs is not None else "—",
        week_delta_pct=fmt_pct(week_delta_pct) if week_delta_pct is not None else "—",
        dep_week=fmt_rub(dep_week),
        plan_progress_pct=f"{plan_pct:.1f} %",
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

    year_start = date(year, 1, 1)
    next_year_start = date(year + 1, 1, 1)

    with db_session() as session:
        # Пополнения за месяц
        dep_month = get_deposits_for_period(
            session,
            start_dt=datetime(year, month, 1),
            end_dt=datetime(next_month_start.year, next_month_start.month, next_month_start.day),
        )

        # Пополнения за год
        dep_year = get_deposits_for_period(
            session,
            start_dt=datetime(year, 1, 1),
            end_dt=datetime(next_year_start.year, next_year_start.month, next_year_start.day),
        )

        # Снапшоты для изменения стоимости за месяц
        start_snap, end_snap = get_month_snapshots(session, year, month)

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
    )

    return render_month_text(ctx)


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
    - просадка от максимума > DRAWDOWN_ALERT_PCT
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

        # Максимум стоимости портфеля на все даты ДО и ВКЛЮЧАЯ текущую дату.
        max_to_last = get_max_value_to_date(session, last_date)

        # Максимум стоимости портфеля на все даты ДО и ВКЛЮЧАЯ предыдущий день.
        max_to_prev = (
            get_max_value_to_date(session, prev_date) if prev_date is not None else None
        )

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

    # Просадка от максимума
    if max_to_last and max_to_last > 0:
        drawdown_curr = (last_value / max_to_last - 1.0) * 100.0
        drawdown_prev = None

        # Для предыдущего дня считаем просадку от максимума на тот момент (max_to_prev),
        # чтобы не было ситуации, когда максимум на самом prev_date не учитывается.
        if prev_value is not None and max_to_prev and max_to_prev > 0:
            drawdown_prev = (prev_value / max_to_prev - 1.0) * 100.0

        threshold = -abs(DRAWDOWN_ALERT_PCT)
        if (
            drawdown_prev is not None
            and drawdown_prev > threshold
            and drawdown_curr <= threshold
        ):
            messages.append(
                f"⚠️ Просадка от максимума достигла примерно {drawdown_curr:.1f} %.\n"
                "Это нормальная часть пути, но полезно быть к этому готовыми психологически."
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
        "/structure — текущая структура портфеля по позициям\n"
        "/history — график стоимости портфеля и суммы пополнений\n"
        "/twr — TWR (time-weighted return) и график по дням\n"
        "/help — эта подсказка\n\n"
        "Автоматически:\n"
        "• каждый день в 19:00 — проверка триггеров (максимумы, просадки)\n"
        "• в последний день месяца — дополнительный отчёт за месяц."
    )
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_today_summary()
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_week_summary()
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_month_summary()
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = build_structure_text()
    await update.message.reply_text(text, parse_mode="Markdown")


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
    Авто-рассылки по расписанию (по TIMEZONE):
    - каждый день в 18:00: проверка триггеров (новый максимум / просадка / годовой план)
    - каждую пятницу в 18:00: недельный отчёт (/week)
    - в последний день месяца в 18:00: месячный отчёт (/month)

    Важно: если Markdown сломается из-за динамических данных — отправляем тем же текстом без разметки.
    """
    now_local = datetime.now(TZ)
    today = now_local.date()
    is_month_end = today == last_day_of_month(today)
    is_friday = today.weekday() == 4  # Monday=0 ... Friday=4

    logger.info(
        "Running scheduled reports for %s (month_end=%s, friday=%s)",
        today.isoformat(),
        is_month_end,
        is_friday,
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

    # Нечего отправлять — выходим тихо.
    if not month_text and not week_text and not triggers:
        logger.info("No messages to send for %s", today.isoformat())
        return

    for chat_id in TARGET_CHAT_IDS:
        # Отдельные try/except на каждое сообщение: чтобы одно падение не глушило всё.
        if is_month_end and month_text:
            try:
                await safe_send_message(context.bot, chat_id, month_text, parse_mode="Markdown")
            except Exception as e:
                logger.error("Error sending month report to chat %s: %s", chat_id, e)

        if is_friday and week_text:
            try:
                await safe_send_message(context.bot, chat_id, week_text, parse_mode="Markdown")
            except Exception as e:
                logger.error("Error sending week report to chat %s: %s", chat_id, e)

        for msg in triggers:
            try:
                await safe_send_message(context.bot, chat_id, msg, parse_mode="Markdown")
            except Exception as e:
                logger.error("Error sending trigger to chat %s: %s", chat_id, e)

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
    app.add_handler(CommandHandler("structure", cmd_structure))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("twr", cmd_twr))

    # Ежедневный джоб
    job_time = time(
        hour=DAILY_SUMMARY_HOUR,
        minute=DAILY_SUMMARY_MINUTE,
        tzinfo=TZ,
    )
    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue не инициализирован. Убедись, что установлен пакет "
            '"python-telegram-bot[job-queue]" и что Application создаётся корректно.'
        )

    app.job_queue.run_daily(daily_job, time=job_time, name="daily_summary")

    logger.info(
        "Bot started. Daily job at %02d:%02d %s",
        DAILY_SUMMARY_HOUR,
        DAILY_SUMMARY_MINUTE,
        TZ_NAME,
    )
    app.run_polling()


if __name__ == "__main__":
    main()
