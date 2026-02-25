from dataclasses import dataclass
from decimal import Decimal
from typing import List
import random

@dataclass(frozen=True)
class WeekContext:
    week_label: str           # например: "10–16 ноября 2025"
    current_value: str        # "337 567 ₽"
    week_delta_abs: str       # "+842 ₽"
    week_delta_pct: str       # "+0.25 %"
    dep_week: str             # "15 000 ₽" — пополнения за неделю
    plan_progress_pct: str    # "87.0 %" — выполнение годового плана по пополнениям
    coupons: Decimal = Decimal('0')
    dividends: Decimal = Decimal('0')
    commissions: Decimal = Decimal('0')
    taxes: Decimal = Decimal('0')

WEEK_TEMPLATES: List[str] = [
    (
        "Привет! Еженедельная сводка за период {week_label} 📆\n\n"
        "Текущая стоимость портфеля: {current_value}.\n"
        "Изменение за неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "За эту неделю вы внесли: {dep_week}.\n"
        "Выполнение годового плана по пополнениям: {plan_progress_pct}."
    ),  # W01
    (
        "Итоги недели {week_label}.\n\n"
        "Ваш семейный портфель оценивается в {current_value}.\n"
        "Динамика за неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения составили {dep_week}.\n"
        "Годовой план выполнен на {plan_progress_pct}.\n\n"
        "Хороших выходных!"
    ),  # W02
    (
        "Неделя {week_label} позади.\n"
        "Баланс: {current_value}.\n"
        "Результат недели: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено средств: {dep_week}.\n"
        "Прогресс плана: {plan_progress_pct}.\n\n"
        "Время отдыхать ☕️"
    ),  # W03
    (
        "📊 Сводка ({week_label}):\n\n"
        "— Капитал: {current_value}\n"
        "— Изменение: {week_delta_abs} ({week_delta_pct})\n"
        "— Депозит: {dep_week}\n"
        "— План года: {plan_progress_pct}\n\n"
        "Отличных выходных вам обоим."
    ),  # W04
    (
        "Коротко о портфеле за {week_label}.\n\n"
        "Текущая сумма: {current_value}.\n"
        "Рынок изменил счёт на {week_delta_abs} ({week_delta_pct}).\n"
        "Вы добавили: {dep_week}.\n"
        "Общий прогресс пополнений: {plan_progress_pct}."
    ),  # W05
    (
        "Привет! Отчёт за неделю {week_label} готов.\n\n"
        "Стоимость активов: {current_value}.\n"
        "Прирост/убыток: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Цель по пополнениям достигнута на {plan_progress_pct}.\n\n"
        "Приятного отдыха!"
    ),  # W06
    (
        "Еженедельный дайджест: {week_label}.\n\n"
        "Сейчас на счету: {current_value}.\n"
        "Изменение стоимости: {week_delta_abs} ({week_delta_pct}).\n"
        "Вклад в будущее за неделю: {dep_week}.\n"
        "Годовой план закрыт на {plan_progress_pct}."
    ),  # W07
    (
        "Финансовые итоги недели {week_label}.\n\n"
        "Портфель: {current_value}.\n"
        "Ваш взнос: {dep_week}.\n"
        "Динамика рынка: {week_delta_abs} ({week_delta_pct}).\n"
        "План пополнений выполнен на {plan_progress_pct}.\n\n"
        "Хороших выходных!"
    ),  # W08
    (
        "Неделя {week_label} завершена.\n\n"
        "Активы: {current_value}.\n"
        "За неделю изменилось на {week_delta_abs} ({week_delta_pct}).\n"
        "Вы инвестировали {dep_week}.\n"
        "Годовой трекер показывает {plan_progress_pct}.\n\n"
        "Можно расслабиться :)"
    ),  # W09
    (
        "Ваш статус на конец недели {week_label} 📈\n\n"
        "Сумма: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Прогресс года: {plan_progress_pct}."
    ),  # W10
    (
        "Сводка по инвестициям ({week_label}).\n\n"
        "Оценка портфеля: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено за неделю: {dep_week}.\n"
        "Выполнение плана: {plan_progress_pct}.\n\n"
        "Всем отличного настроения!"
    ),  # W11
    (
        "Подводим черту под неделей {week_label}.\n\n"
        "Капитал: {current_value}.\n"
        "Рыночная переоценка: {week_delta_abs} ({week_delta_pct}).\n"
        "Кэш-флоу в портфель: {dep_week}.\n"
        "Таргет года выполнен на {plan_progress_pct}."
    ),  # W12
    (
        "Привет! Данные за {week_label}:\n\n"
        "1. Итого: {current_value}\n"
        "2. Изменение: {week_delta_abs} ({week_delta_pct})\n"
        "3. Вклад: {dep_week}\n"
        "4. План: {plan_progress_pct}\n\n"
        "Хороших выходных."
    ),  # W13
    (
        "Статистика за {week_label}.\n\n"
        "Текущий баланс: {current_value}.\n"
        "Недельное изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Годовой прогресс: {plan_progress_pct}.\n\n"
        "Пусть следующая неделя будет удачной!"
    ),  # W14
    (
        "Обзор портфеля за {week_label}.\n\n"
        "Всего средств: {current_value}.\n"
        "Результат инвестиций: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено вами: {dep_week}.\n"
        "План выполнен на {plan_progress_pct}."
    ),  # W15
    (
        "Недельный чек-ап {week_label} ✅\n\n"
        "Портфель: {current_value}.\n"
        "Движение: {week_delta_abs} ({week_delta_pct}).\n"
        "Депозит: {dep_week}.\n"
        "Годовая цель: {plan_progress_pct}.\n\n"
        "Отдыхаем!"
    ),  # W16
    (
        "Результаты периода {week_label}.\n\n"
        "Оценка активов: {current_value}.\n"
        "Изменение стоимости: {week_delta_abs} ({week_delta_pct}).\n"
        "Сумма пополнений: {dep_week}.\n"
        "Выполнение плана: {plan_progress_pct}.\n\n"
        "Хороших выходных вам."
    ),  # W17
    (
        "Инвест-сводка за {week_label}.\n\n"
        "Счёт: {current_value}.\n"
        "Прирост за неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "Прогресс пополнений (год): {plan_progress_pct}."
    ),  # W18
    (
        "Вот как прошла неделя {week_label}:\n\n"
        "Стоимость: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Вы внесли: {dep_week}.\n"
        "План года: {plan_progress_pct}.\n\n"
        "Берегите себя!"
    ),  # W19
    (
        "Данные портфеля, неделя {week_label}.\n\n"
        "Текущая оценка: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Процент выполнения плана: {plan_progress_pct}."
    ),  # W20
    (
        "Привет! Сводка за {week_label} 📉📈\n\n"
        "Баланс: {current_value}.\n"
        "Динамика: {week_delta_abs} ({week_delta_pct}).\n"
        "Вклад: {dep_week}.\n"
        "Годовой план: {plan_progress_pct}.\n\n"
        "Хороших выходных!"
    ),  # W21
    (
        "Отчёт по семейному капиталу ({week_label}).\n\n"
        "Итого: {current_value}.\n"
        "За неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "Годовой трек: {plan_progress_pct}."
    ),  # W22
    (
        "Неделя {week_label}: цифры.\n\n"
        "Портфель: {current_value}.\n"
        "P&L недели: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "План года: {plan_progress_pct}.\n\n"
        "Приятного отдыха."
    ),  # W23
    (
        "Ваши инвестиции на конец недели {week_label}.\n\n"
        "Оценка: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Добавили: {dep_week}.\n"
        "Прогресс плана: {plan_progress_pct}."
    ),  # W24
    (
        "Срез за неделю {week_label}.\n\n"
        "Капитал: {current_value}.\n"
        "Рост/падение: {week_delta_abs} ({week_delta_pct}).\n"
        "Взнос: {dep_week}.\n"
        "Цель пополнений: {plan_progress_pct}.\n\n"
        "Всем добра!"
    ),  # W25
    (
        "Привет! Неделя {week_label} завершилась.\n\n"
        "Сейчас у вас {current_value}.\n"
        "Изменение за неделю составило {week_delta_abs} ({week_delta_pct}).\n"
        "Вы пополнили счёт на {dep_week}.\n"
        "Годовой план выполнен на {plan_progress_pct}."
    ),  # W26
    (
        "Еженедельный отчёт {week_label}.\n\n"
        "Сумма активов: {current_value}.\n"
        "Дельта недели: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "Прогресс года: {plan_progress_pct}.\n\n"
        "Хороших выходных и отличного настроения."
    ),  # W27
    (
        "Быстрая сводка за {week_label} ⚡️\n\n"
        "Баланс: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Депозит: {dep_week}.\n"
        "План: {plan_progress_pct}."
    ),  # W28
    (
        "Неделя {week_label} подошла к концу.\n\n"
        "Стоимость портфеля: {current_value}.\n"
        "За неделю рынок принёс {week_delta_abs} ({week_delta_pct}).\n"
        "Ваши вложения: {dep_week}.\n"
        "Выполнение годового плана: {plan_progress_pct}.\n\n"
        "Пора отдохнуть."
    ),  # W29
    (
        "Финансы за неделю {week_label}.\n\n"
        "Оценка: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "План: {plan_progress_pct}."
    ),  # W30
    (
        "Сводка по портфелю ({week_label}).\n"
        "------------------\n"
        "Итого: {current_value}\n"
        "Динамика: {week_delta_abs} ({week_delta_pct})\n"
        "Внесено: {dep_week}\n"
        "План: {plan_progress_pct}\n\n"
        "Хороших выходных!"
    ),  # W31
    (
        "Привет, вот итоги недели {week_label}.\n\n"
        "Портфель: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесли: {dep_week}.\n"
        "Прогресс года: {plan_progress_pct}.\n\n"
        "Продолжаем движение."
    ),  # W32
    (
        "Недельный отчёт ({week_label}).\n\n"
        "Капитал достиг {current_value}.\n"
        "Изменение за неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнение счёта: {dep_week}.\n"
        "Годовой план: {plan_progress_pct}."
    ),  # W33
    (
        "Ваши активы на {week_label}.\n\n"
        "Стоимость: {current_value}.\n"
        "Вариация за неделю: {week_delta_abs} ({week_delta_pct}).\n"
        "Вы добавили: {dep_week}.\n"
        "Цель выполнена на {plan_progress_pct}.\n\n"
        "Приятных выходных."
    ),  # W34
    (
        "Конец недели {week_label} 🏁\n\n"
        "Баланс: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Депозит: {dep_week}.\n"
        "План года: {plan_progress_pct}."
    ),  # W35
    (
        "Инвестиции, неделя {week_label}.\n\n"
        "Сумма: {current_value}.\n"
        "Рост/снижение: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "Прогресс плана: {plan_progress_pct}.\n\n"
        "Хороших выходных вам обоим!"
    ),  # W36
    (
        "Статус портфеля ({week_label}).\n\n"
        "Текущая оценка: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения за неделю: {dep_week}.\n"
        "План пополнений: {plan_progress_pct}."
    ),  # W37
    (
        "Недельная сводка {week_label}.\n\n"
        "Капитал: {current_value}.\n"
        "Динамика: {week_delta_abs} ({week_delta_pct}).\n"
        "Вклад: {dep_week}.\n"
        "Годовой прогресс: {plan_progress_pct}.\n\n"
        "Набирайтесь сил перед новой неделей."
    ),  # W38
    (
        "Отчёт за период {week_label}.\n\n"
        "Итоговая сумма: {current_value}.\n"
        "Изменение стоимости: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "План года: {plan_progress_pct}."
    ),  # W39
    (
        "Привет! Итоги {week_label}.\n\n"
        "Портфель: {current_value}.\n"
        "Результат недели: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Выполнение плана: {plan_progress_pct}.\n\n"
        "Отличных выходных!"
    ),  # W40
    (
        "Неделя {week_label} пролетела.\n\n"
        "Баланс: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Депозит: {dep_week}.\n"
        "Прогресс плана: {plan_progress_pct}.\n\n"
        "Время для отдыха."
    ),  # W41
    (
        "Дайджест за {week_label}.\n\n"
        "Стоимость: {current_value}.\n"
        "Колебания: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "Годовой план: {plan_progress_pct}."
    ),  # W42
    (
        "Финансы, неделя {week_label}.\n\n"
        "Оценка портфеля: {current_value}.\n"
        "Динамика: {week_delta_abs} ({week_delta_pct}).\n"
        "Вклад: {dep_week}.\n"
        "План: {plan_progress_pct}.\n\n"
        "Всего хорошего!"
    ),  # W43
    (
        "Сводка ({week_label}).\n\n"
        "Капитал: {current_value}.\n"
        "Прирост: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Цель года: {plan_progress_pct}."
    ),  # W44
    (
        "Ваш портфель, неделя {week_label}.\n\n"
        "Сумма: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено: {dep_week}.\n"
        "План выполнен на {plan_progress_pct}.\n\n"
        "Приятного отдыха."
    ),  # W45
    (
        "Недельный итог {week_label} 🗓\n\n"
        "Активы: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Депозит: {dep_week}.\n"
        "Прогресс плана: {plan_progress_pct}."
    ),  # W46
    (
        "Привет! Данные за {week_label}:\n\n"
        "Баланс: {current_value}.\n"
        "Изменение: {week_delta_abs} ({week_delta_pct}).\n"
        "Вклад: {dep_week}.\n"
        "План: {plan_progress_pct}.\n\n"
        "Хороших выходных вам."
    ),  # W47
    (
        "Отчёт по портфелю ({week_label}).\n\n"
        "Оценка: {current_value}.\n"
        "Динамика: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Выполнение плана: {plan_progress_pct}."
    ),  # W48
    (
        "Завершаем неделю {week_label}.\n\n"
        "Счёт: {current_value}.\n"
        "Изменился на {week_delta_abs} ({week_delta_pct}).\n"
        "Внесено вами: {dep_week}.\n"
        "Прогресс года: {plan_progress_pct}.\n\n"
        "Отдыхайте!"
    ),  # W49
    (
        "Финальная сводка недели {week_label}.\n\n"
        "Портфель: {current_value}.\n"
        "Результат: {week_delta_abs} ({week_delta_pct}).\n"
        "Пополнения: {dep_week}.\n"
        "Годовой план: {plan_progress_pct}."
    ),  # W50
]

WEEK_INCOME_EXPENSE_LINES = (
    "\nДоходы: купоны {coupons}, дивиденды {dividends}."
    "\nРасходы: комиссии {commissions}, налоги {taxes}."
)

WEEK_TEMPLATES = [template + WEEK_INCOME_EXPENSE_LINES for template in WEEK_TEMPLATES]

def render_week_text(ctx: WeekContext) -> str:
    template = random.choice(WEEK_TEMPLATES)
    return template.format(**ctx.__dict__)
