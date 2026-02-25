from dataclasses import dataclass
from decimal import Decimal
from typing import List
import random

@dataclass(frozen=True)
class MonthContext:
    month_year_label: str      # например: "ноябрь 2025"
    current_value: str         # "337 567 ₽"
    dep_month: str             # "30 000 ₽" — пополнения за месяц
    dep_year: str              # "348 000 ₽" — пополнения с начала года
    year_plan: str             # "400 000 ₽" — годовой план
    year_progress_pct: str     # "87.0 %" — прогресс по плану
    delta_month_abs: str       # "+4 120 ₽" — изменение стоимости за месяц
    delta_month_pct: str       # "+1.22 %"
    plan_status_phrase: str    # готовая фраза о статусе графика (или пустая строка)
    coupons: Decimal = Decimal('0')
    dividends: Decimal = Decimal('0')
    commissions: Decimal = Decimal('0')
    taxes: Decimal = Decimal('0')

MONTH_TEMPLATES: List[str] = [
    # --- Блок деловых/аналитических шаблонов (M01 - M25) ---
    (
        "📊 Отчёт по портфелю: {month_year_label}\n\n"
        "Текущая оценка: {current_value}.\n"
        "Пополнения за месяц: {dep_month}.\n"
        "Накоплено с начала года: {dep_year} (цель: {year_plan}, выполнено: {year_progress_pct}).\n"
        "Изменение за месяц: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M01
    (
        "Итоги периода {month_year_label}.\n\n"
        "Баланс портфеля: {current_value}.\n"
        "Внесено средств за месяц: {dep_month}.\n"
        "Прогресс пополнений с начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Финансовый результат за месяц: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M02
    (
        "Сводка по инвестиционному счёту за {month_year_label}.\n"
        "---------------------------\n"
        "Стоимость активов: {current_value}\n"
        "Депозит за месяц: {dep_month}\n"
        "Всего с начала года: {dep_year} / {year_plan} ({year_progress_pct})\n"
        "Динамика за месяц: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M03
    (
        "Месячный отчёт ({month_year_label}).\n\n"
        "1. Текущая стоимость: {current_value}\n"
        "2. Взнос за месяц: {dep_month}\n"
        "3. Выполнение годового плана: {year_progress_pct} ({dep_year} из {year_plan})\n"
        "4. Изменение стоимости: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M04
    (
        "Анализ портфеля: {month_year_label}\n\n"
        "Капитализация: {current_value}.\n"
        "Инвестировано в этом месяце: {dep_month}.\n"
        "Суммарно с начала года: {dep_year} (План: {year_plan}, {year_progress_pct}).\n"
        "Доходность за месяц (абс./%): {delta_month_abs} / {delta_month_pct}.\n"
        "{plan_status_phrase}"
    ),  # M05
    (
        "Данные по портфелю за {month_year_label}:\n\n"
        "▪️ Итоговая сумма: {current_value}\n"
        "▪️ Пополнение: {dep_month}\n"
        "▪️ Годовой план: {dep_year} из {year_plan} ({year_progress_pct})\n"
        "▪️ Рост/падение: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M06
    (
        "Краткий отчёт за {month_year_label}.\n\n"
        "Активы оцениваются в {current_value}.\n"
        "За месяц добавлено {dep_month}.\n"
        "Прогресс года: {dep_year} от плана {year_plan} ({year_progress_pct}).\n"
        "Месячное изменение: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M07
    (
        "Инвестиционный отчёт: {month_year_label}\n\n"
        "Текущий капитал: {current_value}.\n"
        "Нетто-приток средств за месяц: {dep_month}.\n"
        "Накопленный итог года: {dep_year} (Целевое значение: {year_plan}, {year_progress_pct}).\n"
        "Вариация стоимости: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M08
    (
        "Статистика счёта за {month_year_label}.\n\n"
        "Баланс: {current_value}.\n"
        "Пополнения (мес.): {dep_month}.\n"
        "Пополнения (год): {dep_year} / {year_plan} ({year_progress_pct}).\n"
        "Результат управления: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M09
    (
        "Информация о состоянии портфеля ({month_year_label}).\n\n"
        "Стоимость: {current_value}\n"
        "Внесено за период: {dep_month}\n"
        "Выполнение плана пополнений: {year_progress_pct} ({dep_year} из {year_plan})\n"
        "Изменение оценки активов: {delta_month_abs} ({delta_month_pct})\n"
        "{plan_status_phrase}"
    ),  # M10
    (
        "Отчётность за {month_year_label}:\n"
        "— Стоимость: {current_value}\n"
        "— Депозит: {dep_month}\n"
        "— План года: {dep_year} / {year_plan} ({year_progress_pct})\n"
        "— Динамика: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M11
    (
        "Финансовый срез за {month_year_label}.\n\n"
        "Портфель достиг отметки {current_value}.\n"
        "За прошедший месяц внесено {dep_month}.\n"
        "С начала года инвестировано {dep_year}, что составляет {year_progress_pct} от плана {year_plan}.\n"
        "Изменение за месяц: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M12
    (
        "Мониторинг портфеля: {month_year_label}.\n\n"
        "Текущее значение: {current_value}.\n"
        "Cash flow за месяц: {dep_month}.\n"
        "YTD пополнения: {dep_year} (Target: {year_plan}, {year_progress_pct}).\n"
        "Monthly P&L: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M13
    (
        "Итоги месяца ({month_year_label}).\n\n"
        "Сумма активов: {current_value}.\n"
        "Вложения: {dep_month}.\n"
        "Годовой трек: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат переоценки: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M14
    (
        "Детализация за {month_year_label}.\n\n"
        "Объём портфеля: {current_value}\n"
        "Пополнение счёта: {dep_month}\n"
        "Исполнение годового бюджета: {dep_year} / {year_plan} ({year_progress_pct})\n"
        "Курсовая разница: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M15
    (
        "Сводка ({month_year_label}).\n\n"
        "Капитал: {current_value}.\n"
        "Инвестиции за месяц: {dep_month}.\n"
        "Инвестиции с начала года: {dep_year} (План: {year_plan}, выполнено на {year_progress_pct}).\n"
        "Прирост/убыток: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M16
    (
        "Отчёт по состоянию активов за {month_year_label}.\n\n"
        "Оценка: {current_value}.\n"
        "Внесение средств: {dep_month}.\n"
        "Статус годового плана: {year_progress_pct} ({dep_year} из {year_plan}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M17
    (
        "Ежемесячный обзор: {month_year_label}.\n\n"
        "Текущий баланс: {current_value}.\n"
        "Сумма пополнений: {dep_month}.\n"
        "С начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Финансовый итог: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M18
    (
        "Параметры портфеля на конец периода: {month_year_label}.\n\n"
        "Стоимость: {current_value}.\n"
        "Внесено (мес): {dep_month}.\n"
        "Внесено (год): {dep_year} (цель {year_plan}, {year_progress_pct}).\n"
        "Динамика стоимости: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M19
    (
        "📊 Результаты за {month_year_label}.\n\n"
        "Общая стоимость: {current_value}.\n"
        "Депозит: {dep_month}.\n"
        "Годовые пополнения: {dep_year} / {year_plan} ({year_progress_pct}).\n"
        "Изменение: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M20
    (
        "Отчёт ({month_year_label}).\n\n"
        "Портфель: {current_value}.\n"
        "Внесено за месяц: {dep_month}.\n"
        "Внесено за год: {dep_year} (План: {year_plan}, прогресс: {year_progress_pct}).\n"
        "Прибыль/Убыток за месяц: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M21
    (
        "Срез данных за {month_year_label}.\n"
        "Тек. стоимость: {current_value}.\n"
        "Пополнение: {dep_month}.\n"
        "Год: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Рост: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M22
    (
        "Инвест-отчёт: {month_year_label}.\n\n"
        "Капитал: {current_value}.\n"
        "Добавлено: {dep_month}.\n"
        "Накопление (YTD): {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат месяца: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M23
    (
        "Обзор счёта ({month_year_label}).\n\n"
        "Сумма: {current_value}\n"
        "Вклад за месяц: {dep_month}\n"
        "Прогресс года: {year_progress_pct} ({dep_year} из {year_plan})\n"
        "Изменение: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}"
    ),  # M24
    (
        "Финальные цифры за {month_year_label}.\n\n"
        "Оценка портфеля: {current_value}.\n"
        "Внесено средств: {dep_month}.\n"
        "Пополнения с начала года: {dep_year} / {year_plan} ({year_progress_pct}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}"
    ),  # M25

    # --- Блок мотивирующих/тёплых шаблонов (M26 - M50) ---
    (
        "🚀 Отчёт за {month_year_label} готов!\n\n"
        "Ваш портфель сейчас: {current_value}.\n"
        "За этот месяц вы инвестировали {dep_month} — отличная работа!\n"
        "С начала года накоплено {dep_year} из цели в {year_plan} ({year_progress_pct}).\n"
        "Результат работы портфеля: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M26
    (
        "Итоги месяца: {month_year_label} ✨\n\n"
        "Текущий капитал: {current_value}.\n"
        "Копилка пополнилась на {dep_month}.\n"
        "Годовая цель выполнена на {year_progress_pct} ({dep_year} из {year_plan}).\n"
        "Движение рынка принесло {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Продолжаем путь к финансовой свободе!"
    ),  # M27
    (
        "Ваши успехи за {month_year_label} 📈\n\n"
        "Портфель вырос до {current_value}.\n"
        "В этом месяце вы отложили {dep_month}.\n"
        "Всего с начала года внесено {dep_year} (Цель: {year_plan}, {year_progress_pct}).\n"
        "Изменение стоимости за месяц составило {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Дисциплина — ключ к успеху!"
    ),  # M28
    (
        "Отчёт по портфелю за {month_year_label}.\n\n"
        "💰 Капитал: {current_value}\n"
        "➕ Пополнили на: {dep_month}\n"
        "🗓 Прогресс года: {dep_year} из {year_plan} ({year_progress_pct})\n"
        "📊 Результат: {delta_month_abs} ({delta_month_pct})\n\n"
        "{plan_status_phrase}\n"
        "Ещё один шаг сделан. Так держать!"
    ),  # M29
    (
        "Месяц {month_year_label} позади.\n\n"
        "Сейчас на счету: {current_value}.\n"
        "Ваш вклад в будущее за месяц: {dep_month}.\n"
        "С начала года уже собрано {dep_year} из запланированных {year_plan} ({year_progress_pct}).\n"
        "Портфель изменился на {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Инвестиции любят терпеливых."
    ),  # M30
    (
        "Новости вашего капитала за {month_year_label} 🗞\n\n"
        "Стоимость портфеля: {current_value}.\n"
        "Вы инвестировали {dep_month} за этот месяц.\n"
        "Годовой план выполнен на {year_progress_pct} ({dep_year} из {year_plan}).\n"
        "Динамика за месяц: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M31
    (
        "Вот как прошёл {month_year_label}:\n\n"
        "Ваши активы: {current_value}.\n"
        "Пополнение: {dep_month} — грамотное решение.\n"
        "С начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Рыночное изменение: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Сложный процент работает на вас!"
    ),  # M32
    (
        "Инвестиционный дайджест: {month_year_label} 💼\n\n"
        "Сумма на счёте: {current_value}.\n"
        "Вклад за месяц: {dep_month}.\n"
        "Годовой прогресс: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Изменение баланса: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Вы создаёте своё будущее."
    ),  # M33
    (
        "Результаты месяца {month_year_label}.\n\n"
        "Портфель оценивается в {current_value}.\n"
        "За месяц вы добавили {dep_month}.\n"
        "По плану года уже {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Регулярность важнее всего."
    ),  # M34
    (
        "Ваш отчёт за {month_year_label} 📥\n\n"
        "Текущая стоимость: {current_value}.\n"
        "Инвестировано в этом месяце: {dep_month}.\n"
        "Накоплено с начала года: {dep_year} (цель: {year_plan}, {year_progress_pct}).\n"
        "Прирост/снижение: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M35
    (
        "Статистика за {month_year_label} ✅\n\n"
        "Баланс: {current_value}.\n"
        "За месяц внесено: {dep_month}.\n"
        "С начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат за месяц: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Каждая сумма приближает к цели!"
    ),  # M36
    (
        "Обзор инвестиций: {month_year_label}.\n\n"
        "Активы: {current_value}.\n"
        "Пополнение: {dep_month}.\n"
        "Годовой план: {year_progress_pct} выполнено ({dep_year} из {year_plan}).\n"
        "Изменение оценки: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Продолжаем движение вперёд 🚀"
    ),  # M37
    (
        "Финансовые итоги {month_year_label}.\n\n"
        "Размер капитала: {current_value}.\n"
        "Ваш вклад за месяц: {dep_month}.\n"
        "С начала года накоплено {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат управления: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Время — лучший друг инвестора."
    ),  # M38
    (
        "Отчёт за {month_year_label}.\n\n"
        "Стоимость портфеля: {current_value}.\n"
        "За месяц внесено {dep_month} — кирпичик в фундамент вашего капитала.\n"
        "План года: {dep_year} / {year_plan} ({year_progress_pct}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M39
    (
        "Портфель за {month_year_label} 📊\n\n"
        "Всего средств: {current_value}.\n"
        "Пополнения месяца: {dep_month}.\n"
        "Пополнения с начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат за месяц: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Уверенный шаг к целям!"
    ),  # M40
    (
        "Ваши инвестиции в {month_year_label}.\n\n"
        "Текущая оценка: {current_value}.\n"
        "Вложения за месяц: {dep_month}.\n"
        "Прогресс по году: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Динамика портфеля: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Деньги должны работать."
    ),  # M41
    (
        "Итоги {month_year_label} подведены.\n\n"
        "💰 Капитал: {current_value}.\n"
        "➕ Пополнение: {dep_month}.\n"
        "🎯 Цель года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "📉📈 Изменение: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M42
    (
        "Отчёт за {month_year_label}: всё по плану?\n\n"
        "Портфель: {current_value}.\n"
        "Внесено за месяц: {dep_month}.\n"
        "Внесено за год: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат месяца: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Главное — не останавливаться."
    ),  # M43
    (
        "Месячный срез ({month_year_label}) 🗓\n\n"
        "Сумма на счёте: {current_value}.\n"
        "Ваши инвестиции за месяц: {dep_month}.\n"
        "Выполнение плана на год: {year_progress_pct} ({dep_year} из {year_plan}).\n"
        "Прирост/убыток: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}"
    ),  # M44
    (
        "Ваш капитал за {month_year_label}.\n\n"
        "Стоимость: {current_value}.\n"
        "Депозит за месяц: {dep_month}.\n"
        "Накоплено с начала года: {dep_year} (Цель: {year_plan}, {year_progress_pct}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Путь в тысячу миль продолжается."
    ),  # M45
    (
        "Сводка за {month_year_label} 💎\n\n"
        "Текущий баланс: {current_value}.\n"
        "Внесли в этом месяце: {dep_month}.\n"
        "Всего за год: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Результат месяца: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Отличная привычка — инвестировать!"
    ),  # M46
    (
        "Инвестиции: {month_year_label}.\n\n"
        "Портфель: {current_value}.\n"
        "Пополнения: {dep_month}.\n"
        "Годовой прогресс: {dep_year} / {year_plan} ({year_progress_pct}).\n"
        "Динамика: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Ваше будущее скажет вам спасибо."
    ),  # M47
    (
        "Отчёт по счёту за {month_year_label}.\n\n"
        "Оценка активов: {current_value}.\n"
        "Инвестировано: {dep_month}.\n"
        "План на год выполнен на {year_progress_pct} ({dep_year} из {year_plan}).\n"
        "Изменение: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Терпение и труд капитал перетрут :)"
    ),  # M48
    (
        "Итоги месяца {month_year_label}.\n\n"
        "Сумма: {current_value}.\n"
        "Вклад за месяц: {dep_month}.\n"
        "Вклад за год: {dep_year} (из {year_plan}, {year_progress_pct}).\n"
        "Рост/падение: {delta_month_abs} ({delta_month_pct}).\n"
        "{plan_status_phrase}\n\n"
        "Держим курс!"
    ),  # M49
    (
        "Ваш финансовый результат за {month_year_label} 🏁\n\n"
        "Портфель: {current_value}.\n"
        "За месяц добавили: {dep_month}.\n"
        "С начала года: {dep_year} из {year_plan} ({year_progress_pct}).\n"
        "Изменение стоимости: {delta_month_abs} ({delta_month_pct}).\n\n"
        "{plan_status_phrase}\n"
        "Всё идёт своим чередом."
    ),  # M50
]

MONTH_INCOME_EXPENSE_LINES = (
    "\nДоходы: купоны {coupons}, дивиденды {dividends}."
    "\nРасходы: комиссии {commissions}, налоги {taxes}."
)

MONTH_TEMPLATES = [template + MONTH_INCOME_EXPENSE_LINES for template in MONTH_TEMPLATES]

def render_month_text(ctx: MonthContext) -> str:
    template = random.choice(MONTH_TEMPLATES)
    return template.format(
        month_year_label=ctx.month_year_label,
        current_value=ctx.current_value,
        dep_month=ctx.dep_month,
        dep_year=ctx.dep_year,
        year_plan=ctx.year_plan,
        year_progress_pct=ctx.year_progress_pct,
        delta_month_abs=ctx.delta_month_abs,
        delta_month_pct=ctx.delta_month_pct,
        plan_status_phrase=ctx.plan_status_phrase,
        coupons=ctx.coupons,
        dividends=ctx.dividends,
        commissions=ctx.commissions,
        taxes=ctx.taxes,
    ).strip()
