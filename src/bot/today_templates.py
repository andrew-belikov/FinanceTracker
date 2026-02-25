from dataclasses import dataclass
from decimal import Decimal
from typing import List
import random

@dataclass(frozen=True)
class TodayContext:
    snapshot_dt: str      # "17.11.25 23:30"
    current_value: str    # "337 567 ₽"
    delta_abs: str        # "+1 235 ₽"
    delta_pct: str        # "+0.37 %"
    pnl_abs: str          # "-10 294 ₽"
    pnl_pct: str          # "-3.0 %"
    coupons: Decimal = Decimal('0')
    dividends: Decimal = Decimal('0')
    commissions: Decimal = Decimal('0')
    taxes: Decimal = Decimal('0')

TODAY_TEMPLATES: List[str] = [
    (
        "Свежая сводка ({snapshot_dt})\n\n"
        "Портфель стоит {current_value}.\n"
        "За сутки изменение: {delta_abs} ({delta_pct}).\n"
        "Совокупный результат за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T01
    (
        "Обновление портфеля на {snapshot_dt}.\n\n"
        "Текущая стоимость: {current_value}.\n"
        "Движение за день: {delta_abs} ({delta_pct}).\n"
        "Общая доходность: {pnl_abs} ({pnl_pct})."
    ),  # T02
    (
        "Сводка по счёту ({snapshot_dt}).\n\n"
        "Баланс: {current_value}.\n"
        "К вчерашнему дню: {delta_abs} ({delta_pct}).\n"
        "Результат за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T03
    (
        "Данные на {snapshot_dt}:\n\n"
        "— Оценка активов: {current_value}\n"
        "— Изменение (24ч): {delta_abs} ({delta_pct})\n"
        "— Общий итог: {pnl_abs} ({pnl_pct})"
    ),  # T04
    (
        "Статус инвестиций ({snapshot_dt}).\n\n"
        "Ваш капитал: {current_value}.\n"
        "Динамика за сутки: {delta_abs} ({delta_pct}).\n"
        "Прибыль/убыток за весь период: {pnl_abs} ({pnl_pct})."
    ),  # T05
    (
        "Мониторинг портфеля: {snapshot_dt}.\n\n"
        "Сумма на счёте: {current_value}.\n"
        "Суточное изменение: {delta_abs} ({delta_pct}).\n"
        "Накопленная доходность: {pnl_abs} ({pnl_pct})."
    ),  # T06
    (
        "Экспресс-сводка ({snapshot_dt}).\n\n"
        "Стоимость: {current_value}.\n"
        "Сегодняшний результат: {delta_abs} ({delta_pct}).\n"
        "Глобальный результат: {pnl_abs} ({pnl_pct})."
    ),  # T07
    (
        "Отчёт по состоянию на {snapshot_dt}.\n\n"
        "Активы оцениваются в {current_value}.\n"
        "Изменение к вчера: {delta_abs} ({delta_pct}).\n"
        "Итог управления: {pnl_abs} ({pnl_pct})."
    ),  # T08
    (
        "Информация по счёту ({snapshot_dt}).\n\n"
        "Текущий баланс: {current_value}.\n"
        "За день: {delta_abs} ({delta_pct}).\n"
        "За всё время: {pnl_abs} ({pnl_pct})."
    ),  # T09
    (
        "Дневной срез: {snapshot_dt}.\n\n"
        "Портфель: {current_value}.\n"
        "Дельта (сутки): {delta_abs} ({delta_pct}).\n"
        "Дельта (всего): {pnl_abs} ({pnl_pct})."
    ),  # T10
    (
        "Сводка от {snapshot_dt}.\n\n"
        "Оценка портфеля: {current_value}.\n"
        "Вариация за день: {delta_abs} ({delta_pct}).\n"
        "Общий финансовый результат: {pnl_abs} ({pnl_pct})."
    ),  # T11
    (
        "📊 Состояние на {snapshot_dt}.\n\n"
        "Сумма: {current_value}.\n"
        "Изменение за 24ч: {delta_abs} ({delta_pct}).\n"
        "P&L за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T12
    (
        "Проверка портфеля ({snapshot_dt}).\n\n"
        "Стоимость сейчас: {current_value}.\n"
        "Результат дня: {delta_abs} ({delta_pct}).\n"
        "Результат всего периода: {pnl_abs} ({pnl_pct})."
    ),  # T13
    (
        "Актуальные цифры на {snapshot_dt}:\n\n"
        "1. Баланс: {current_value}\n"
        "2. Сутки: {delta_abs} ({delta_pct})\n"
        "3. Итого: {pnl_abs} ({pnl_pct})"
    ),  # T14
    (
        "Ваши инвестиции ({snapshot_dt}).\n\n"
        "Оценка: {current_value}.\n"
        "К вчерашнему закрытию: {delta_abs} ({delta_pct}).\n"
        "Общая динамика: {pnl_abs} ({pnl_pct})."
    ),  # T15
    (
        "Сводка ({snapshot_dt}).\n\n"
        "Капитал: {current_value}.\n"
        "Дневное изменение: {delta_abs} ({delta_pct}).\n"
        "Доходность за весь срок: {pnl_abs} ({pnl_pct})."
    ),  # T16
    (
        "Обзор счёта на {snapshot_dt}.\n\n"
        "Стоимость активов: {current_value}.\n"
        "Изменилось за день на {delta_abs} ({delta_pct}).\n"
        "Изменилось за всё время на {pnl_abs} ({pnl_pct})."
    ),  # T17
    (
        "Данные трекера ({snapshot_dt}).\n\n"
        "Баланс: {current_value}.\n"
        "Сутки: {delta_abs} ({delta_pct}).\n"
        "Весь период: {pnl_abs} ({pnl_pct})."
    ),  # T18
    (
        "Инвест-отчёт: {snapshot_dt}.\n\n"
        "Текущая сумма: {current_value}.\n"
        "Результат за сегодня: {delta_abs} ({delta_pct}).\n"
        "Накопленный результат: {pnl_abs} ({pnl_pct})."
    ),  # T19
    (
        "Срез на {snapshot_dt} 📉📈\n\n"
        "Оценка: {current_value}.\n"
        "День: {delta_abs} ({delta_pct}).\n"
        "Всего: {pnl_abs} ({pnl_pct})."
    ),  # T20
    (
        "Статистика ({snapshot_dt}).\n\n"
        "Портфель: {current_value}.\n"
        "Дневная дельта: {delta_abs} ({delta_pct}).\n"
        "Общая дельта: {pnl_abs} ({pnl_pct})."
    ),  # T21
    (
        "Финансы на {snapshot_dt}.\n\n"
        "Капитал: {current_value}.\n"
        "Рост/падение за сутки: {delta_abs} ({delta_pct}).\n"
        "Рост/падение итого: {pnl_abs} ({pnl_pct})."
    ),  # T22
    (
        "Ежедневная сводка ({snapshot_dt}).\n\n"
        "Активы: {current_value}.\n"
        "Сдвиг за сутки: {delta_abs} ({delta_pct}).\n"
        "Общий итог инвестиций: {pnl_abs} ({pnl_pct})."
    ),  # T23
    (
        "Бот: данные на {snapshot_dt}.\n\n"
        "Стоимость: {current_value}.\n"
        "Суточное колебание: {delta_abs} ({delta_pct}).\n"
        "Результат с начала: {pnl_abs} ({pnl_pct})."
    ),  # T24
    (
        "Кратко о портфеле ({snapshot_dt}):\n"
        "• {current_value}\n"
        "• Сутки: {delta_abs} ({delta_pct})\n"
        "• Всего: {pnl_abs} ({pnl_pct})"
    ),  # T25
    (
        "Обновление ({snapshot_dt}).\n\n"
        "Текущий размер портфеля: {current_value}.\n"
        "Изменение к вчера: {delta_abs} ({delta_pct}).\n"
        "Изменение за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T26
    (
        "Сводка состояния ({snapshot_dt}).\n\n"
        "Баланс: {current_value}.\n"
        "Динамика дня: {delta_abs} ({delta_pct}).\n"
        "Динамика полная: {pnl_abs} ({pnl_pct})."
    ),  # T27
    (
        "Ваш счёт на {snapshot_dt}.\n\n"
        "Оценка: {current_value}.\n"
        "Результат за 24 часа: {delta_abs} ({delta_pct}).\n"
        "Совокупный P&L: {pnl_abs} ({pnl_pct})."
    ),  # T28
    (
        "Дайджест ({snapshot_dt}).\n\n"
        "Сумма: {current_value}.\n"
        "Сутки: {delta_abs} ({delta_pct}).\n"
        "Итого: {pnl_abs} ({pnl_pct})."
    ),  # T29
    (
        "Показатели на {snapshot_dt}.\n\n"
        "Стоимость: {current_value}.\n"
        "Изменение сегодня: {delta_abs} ({delta_pct}).\n"
        "Изменение всего: {pnl_abs} ({pnl_pct})."
    ),  # T30
    (
        "Свежие цифры ({snapshot_dt}).\n\n"
        "Портфель: {current_value}.\n"
        "День: {delta_abs} ({delta_pct}).\n"
        "Всего: {pnl_abs} ({pnl_pct})."
    ),  # T31
    (
        "Отчёт робота ({snapshot_dt}).\n\n"
        "Оценка активов: {current_value}.\n"
        "Суточный результат: {delta_abs} ({delta_pct}).\n"
        "Накопленный результат: {pnl_abs} ({pnl_pct})."
    ),  # T32
    (
        "Мониторинг ({snapshot_dt}) 📊\n\n"
        "Капитал: {current_value}.\n"
        "Дельта дня: {delta_abs} ({delta_pct}).\n"
        "Дельта всего: {pnl_abs} ({pnl_pct})."
    ),  # T33
    (
        "Инфо по портфелю ({snapshot_dt}).\n\n"
        "Стоимость: {current_value}.\n"
        "Изменение (день): {delta_abs} ({delta_pct}).\n"
        "Изменение (общ): {pnl_abs} ({pnl_pct})."
    ),  # T34
    (
        "Сводка на момент {snapshot_dt}.\n\n"
        "Баланс: {current_value}.\n"
        "Прирост за сутки: {delta_abs} ({delta_pct}).\n"
        "Прирост за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T35
    (
        "Состояние счёта: {snapshot_dt}.\n\n"
        "Сумма: {current_value}.\n"
        "Результат дня: {delta_abs} ({delta_pct}).\n"
        "Глобальный P&L: {pnl_abs} ({pnl_pct})."
    ),  # T36
    (
        "Данные за {snapshot_dt}.\n\n"
        "Оценка: {current_value}.\n"
        "Сутки: {delta_abs} ({delta_pct}).\n"
        "Всего: {pnl_abs} ({pnl_pct})."
    ),  # T37
    (
        "Текущий срез ({snapshot_dt}).\n\n"
        "Портфель: {current_value}.\n"
        "Динамика (24ч): {delta_abs} ({delta_pct}).\n"
        "Динамика (Total): {pnl_abs} ({pnl_pct})."
    ),  # T38
    (
        "Сводка изменений ({snapshot_dt}).\n\n"
        "Активы: {current_value}.\n"
        "День: {delta_abs} ({delta_pct}).\n"
        "Весь срок: {pnl_abs} ({pnl_pct})."
    ),  # T39
    (
        "Ваш капитал на {snapshot_dt}.\n\n"
        "Итого: {current_value}.\n"
        "Изменение к вчера: {delta_abs} ({delta_pct}).\n"
        "Результат за всё время: {pnl_abs} ({pnl_pct})."
    ),  # T40
    (
        "Апдейт портфеля ({snapshot_dt}).\n\n"
        "Стоимость: {current_value}.\n"
        "Колебание за сутки: {delta_abs} ({delta_pct}).\n"
        "Доходность портфеля: {pnl_abs} ({pnl_pct})."
    ),  # T41
    (
        "Информация ({snapshot_dt}) ℹ️\n\n"
        "Счёт: {current_value}.\n"
        "Сутки: {delta_abs} ({delta_pct}).\n"
        "Всего: {pnl_abs} ({pnl_pct})."
    ),  # T42
    (
        "Результаты на {snapshot_dt}.\n\n"
        "Оценка: {current_value}.\n"
        "День: {delta_abs} ({delta_pct}).\n"
        "Общий итог: {pnl_abs} ({pnl_pct})."
    ),  # T43
    (
        "Сводка данных ({snapshot_dt}).\n\n"
        "Баланс: {current_value}.\n"
        "Изменение сегодня: {delta_abs} ({delta_pct}).\n"
        "Изменение вообще: {pnl_abs} ({pnl_pct})."
    ),  # T44
    (
        "Портфель на {snapshot_dt}.\n\n"
        "Сумма: {current_value}.\n"
        "Результат (24ч): {delta_abs} ({delta_pct}).\n"
        "Результат (Total): {pnl_abs} ({pnl_pct})."
    ),  # T45
    (
        "Свежая информация ({snapshot_dt}).\n\n"
        "Стоимость: {current_value}.\n"
        "К вчерашнему дню: {delta_abs} ({delta_pct}).\n"
        "За весь период: {pnl_abs} ({pnl_pct})."
    ),  # T46
    (
        "Чек-ап ({snapshot_dt}).\n\n"
        "Активы: {current_value}.\n"
        "День: {delta_abs} ({delta_pct}).\n"
        "Итого: {pnl_abs} ({pnl_pct})."
    ),  # T47
    (
        "Статус ({snapshot_dt}).\n\n"
        "Портфель: {current_value}.\n"
        "Суточная дельта: {delta_abs} ({delta_pct}).\n"
        "Общая дельта: {pnl_abs} ({pnl_pct})."
    ),  # T48
    (
        "Отчётность на {snapshot_dt}.\n\n"
        "Оценка: {current_value}.\n"
        "Изменение за день: {delta_abs} ({delta_pct}).\n"
        "Прибыль/убыток всего: {pnl_abs} ({pnl_pct})."
    ),  # T49
    (
        "Финальная сводка ({snapshot_dt}).\n\n"
        "Капитал: {current_value}.\n"
        "Сутки: {delta_abs} ({delta_pct}).\n"
        "Всё время: {pnl_abs} ({pnl_pct})."
    ),  # T50
]

TODAY_INCOME_EXPENSE_LINES = (
    "\nДоходы: купоны {coupons}, дивиденды {dividends}."
    "\nРасходы: комиссии {commissions}, налоги {taxes}."
)

TODAY_TEMPLATES = [template + TODAY_INCOME_EXPENSE_LINES for template in TODAY_TEMPLATES]

def render_today_text(ctx: TodayContext) -> str:
    template = random.choice(TODAY_TEMPLATES)
    return template.format(
        snapshot_dt=ctx.snapshot_dt,
        current_value=ctx.current_value,
        delta_abs=ctx.delta_abs,
        delta_pct=ctx.delta_pct,
        pnl_abs=ctx.pnl_abs,
        pnl_pct=ctx.pnl_pct,
        coupons=ctx.coupons,
        dividends=ctx.dividends,
        commissions=ctx.commissions,
        taxes=ctx.taxes,
    )
