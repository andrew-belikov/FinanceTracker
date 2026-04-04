# Bot Decomposition Plan

## Цель

Поэтапно распилить [`src/bot/bot.py`](/Users/andrew/Dev/FinanceTracker/src/bot/bot.py) на отдельные модули без изменения внешнего поведения бота.

Целевые модули:

- `src/bot/handlers.py`
- `src/bot/queries.py`
- `src/bot/charts.py`
- `src/bot/dataset.py`
- `src/bot/jobs.py`

Обязательные supporting-модули, без которых распил будет либо циклическим, либо слишком хрупким:

- `src/bot/runtime.py` — общая конфигурация, logger, DB session, shared helpers
- `src/bot/services.py` — бизнес-логика и текстовые builders, которые нужны и handlers, и jobs

## Важные ограничения

- Не менять поведение команд, тексты ответов, SQL-смысл запросов и расписание JobQueue.
- Не переходить на package-style imports вида `from bot.handlers import ...`.
  Причина: контейнер из [`docker/Dockerfile.bot`](/Users/andrew/Dev/FinanceTracker/docker/Dockerfile.bot) копирует `src/bot/*` плоско в `/app` и запускает `python bot.py`.
- Использовать sibling-imports: `from runtime import ...`, `from queries import ...`.
- `src/bot/bot.py` должен остаться entrypoint-композитором до самого конца, потому что [`src/bot/entrypoint.py`](/Users/andrew/Dev/FinanceTracker/src/bot/entrypoint.py) делает `os.execvp("python", ["python", "-u", "bot.py"])`.
- Каждый шаг ниже должен заканчиваться одним самостоятельным PR, после которого проект можно коммитить и делать пробную сборку.

## Текущее состояние `bot.py`

- Размер файла: `5682` строки.
- Количество `def` / `async def` / `class`: `153`.
- В одном модуле смешаны:
  - env/config/bootstrap
  - Telegram handlers
  - JobQueue jobs
  - SQL access
  - текстовые summary/builders
  - rebalance/invest logic
  - `matplotlib`
  - dataset export и ZIP

Крупные кластеры по текущим секциям:

- Shared helpers и formatting: около строк `278-911`
- DB queries: около строк `916-2287`
- Business/reporting/rebalance logic: около строк `2292-3399`, `3775-4044`, `4505-4567`
- Charts: около строк `3400-3774`, `3950-4044`
- Dataset export: около строк `4045-4502`
- Handlers: около строк `4606-4929`
- Jobs: около строк `4570-4600`, `4935-5215`
- App wiring: около строк `5216-5294`

## Правила зависимости между модулями

- `runtime.py` не импортирует `handlers.py`, `jobs.py`, `charts.py`, `dataset.py`.
- `queries.py` не импортирует Telegram и `matplotlib`.
- `charts.py` не импортирует Telegram.
- `dataset.py` не импортирует Telegram.
- `jobs.py` не импортирует `handlers.py`.
- `handlers.py` не импортирует `jobs.py`.
- `bot.py` является только composition root: собирает `Application`, регистрирует handlers/jobs и запускает polling.

Итоговый граф зависимостей должен быть таким:

- `runtime -> none`
- `queries -> runtime`
- `services -> runtime, queries, templates`
- `charts -> runtime, queries`
- `dataset -> runtime, queries, services`
- `jobs -> runtime, queries, services`
- `handlers -> runtime, queries, services, charts, dataset`
- `bot -> runtime, handlers, jobs`

## Общая схема проверки для каждого PR

Минимум после каждого шага:

```bash
python3 -m compileall src
python3 -m unittest discover -s tests -p "test_*.py"
docker compose config > /dev/null
```

Рекомендуемая пробная сборка после каждого шага:

```bash
docker compose build bot
```

Если PR меняет только тесты и Python-модули, но не меняет compose/runtime wiring, `docker compose build bot` можно делать после коммита как отдельный smoke step.

## PR 1. Вынести shared runtime foundation в `runtime.py`

### Цель

Сначала создать стабильный общий слой, от которого потом смогут зависеть `queries.py`, `services.py`, `jobs.py` и `handlers.py` без циклических импортов.

### Что переносим из `bot.py`

- Конфиг из env:
  - `TELEGRAM_BOT_TOKEN`
  - `ALLOWED_USER_IDS`, `TARGET_CHAT_IDS`
  - `ACCOUNT_FRIENDLY_NAME`
  - `TZ_NAME`, `TZ`, `HOST_TZ`
  - `JOBQUEUE_SMOKE_TEST_ON_START`, `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS`
  - `PLAN_ANNUAL_CONTRIB_RUB`
  - `TINKOFF_ACCOUNT_ID`
  - `DB_*`, `DB_DSN`
- DB bootstrap:
  - `engine`
  - `SessionLocal`
  - `db_session`
- Logging bootstrap:
  - `configure_logging()`
  - `logger`
- Общие shared constants:
  - operation type tuples
  - `EXECUTED_OPERATION_STATE`
  - `OPERATIONS_DEDUP_CTE`
  - `REPORTING_ACCOUNT_UNAVAILABLE_TEXT`
  - `TARGETS_USAGE_TEXT`
  - `INVEST_USAGE_TEXT`
  - `REBALANCE_FEATURE_UNAVAILABLE_TEXT`
  - `REBALANCE_TARGETS_NOT_CONFIGURED_TEXT`
- Shared helpers:
  - `safe_send_message`
  - `is_authorized`
  - `log_update_received`
  - `fmt_*`
  - `to_local_market_date`
  - `to_iso_datetime`
  - `decimal_to_str`
  - `json_default`
  - `write_csv_file`
  - `normalize_decimal`
  - `last_day_of_month`
  - month-name dictionaries

### Что пока оставляем в `bot.py`

- Все SQL functions
- Вся бизнес-логика команд
- Все handlers/jobs
- Все chart/dataset builders

### Инструкции

1. Создать `src/bot/runtime.py`.
2. Перенести туда только инфраструктурный и truly shared код.
3. В `bot.py` заменить локальные определения на явные импорты из `runtime.py`.
4. Не переносить в этот PR dataset/rebalance-specific helpers, которые уже зашиты в AST-based tests.
5. Обновить [`tests/test_runtime_logging_guardrails.py`](/Users/andrew/Dev/FinanceTracker/tests/test_runtime_logging_guardrails.py): добавить `runtime.py` в `RUNTIME_FILES`.

### Критерий готовности

- `bot.py` больше не содержит env/bootstrap/session/logger plumbing.
- Нет циклических импортов.
- Поведение полностью прежнее.

## PR 2. Вынести SQL слой в `queries.py`

### Цель

Выделить весь DB access в отдельный модуль, чтобы потом `services.py`, `charts.py`, `dataset.py` и `jobs.py` пользовались только query API, а не raw SQL внутри себя.

### Что переносим

Из секции `DB QUERIES (CORE)` и смежных DB-write helper’ов:

- `get_latest_snapshot_account_id`
- `normalize_reporting_account_id`
- `choose_reporting_account_id`
- `resolve_reporting_account_id`
- `_is_undefined_table_error`
- все `get_*` функции, которые читают БД
- `replace_rebalance_targets`
- `bootstrap_invest_notifications`
- `get_pending_invest_notifications`
- `mark_invest_notification_sent`

Дополнительно в этом же PR нужно разбить смешанные query+logic места на “raw read” и “service composition”, чтобы следующий PR был проще:

- для годового diff подготовить raw query helpers вместо SQL внутри `compute_positions_diff_grouped`
- для income notifications вынести query helpers:
  - `get_unnotified_income_events(...)`
  - `mark_income_event_notified(...)`

### Инструкции

1. Создать `src/bot/queries.py`.
2. Перенести туда SQL functions без изменения самих SQL.
3. В `bot.py` заменить прямые определения на импорты из `queries.py`.
4. Не перемещать в `queries.py` форматирование текста и Telegram-specific код.
5. Обновить [`tests/test_reporting_metrics.py`](/Users/andrew/Dev/FinanceTracker/tests/test_reporting_metrics.py):
   - resolution helpers должны загружаться из `queries.py`, а не из `bot.py`.
6. Если в `bot.py` ещё остаются composite-функции, использующие raw SQL, подготовить их к следующему шагу через новые low-level query helpers.

### Критерий готовности

- В `bot.py` нет raw SQL query functions.
- `queries.py` не содержит Telegram и `matplotlib`.
- Поведение и SQL shape не изменены.

## PR 3. Вынести service layer в `services.py`

### Цель

Создать промежуточный слой бизнес-логики, который нужен и handlers, и jobs. Без этого `handlers.py` и `jobs.py` будут вынуждены импортировать `bot.py`, что снова создаст монолит.

### Что переносим

Pure/composite business logic:

- `build_help_text`
- TWR math:
  - `compute_twr_series`
  - `compute_xnpv`
  - `compute_xirr`
  - `project_run_rate_value`
  - `compute_twr_timeseries`
  - `compute_portfolio_xirr_and_run_rate`
  - `render_twr_summary_text`
- summary builders:
  - `build_today_summary`
  - `build_week_summary`
  - `build_month_summary`
  - `build_year_summary`
  - `build_structure_text`
  - `build_triggers_messages`
- rebalance/invest domain:
  - rebalance constants
  - `parse_decimal_input`
  - `parse_rebalance_targets_args`
  - `aggregate_rebalance_values_by_class`
  - `compute_rebalance_plan`
  - `compute_invest_plan`
  - `build_targets_text_for_account`
  - `build_rebalance_text_for_account`
  - `build_invest_text_for_account`
  - supporting formatters
- positions diff logic:
  - `compute_positions_diff_lines`
  - `compute_positions_diff_grouped`

### Инструкции

1. Создать `src/bot/services.py`.
2. Перенести туда всё, что:
   - строит текст/summary,
   - агрегирует данные,
   - использует query functions,
   - но не должно знать про Telegram `Update`/`Context`.
3. Убедиться, что `services.py` опирается только на:
   - `runtime.py`
   - `queries.py`
   - template modules (`today_templates.py`, `week_templates.py`, `month_templates.py`)
4. Перевести AST-based tests:
   - [`tests/test_reporting_metrics.py`](/Users/andrew/Dev/FinanceTracker/tests/test_reporting_metrics.py): TWR math из `services.py`
   - [`tests/test_rebalance_features.py`](/Users/andrew/Dev/FinanceTracker/tests/test_rebalance_features.py): rebalance helpers и `build_help_text` из `services.py`
   - [`tests/test_month_positions_diff.py`](/Users/andrew/Dev/FinanceTracker/tests/test_month_positions_diff.py): `compute_positions_diff_lines` из `services.py`
5. После этого `bot.py` должен использовать service imports вместо локальных builders.

### Критерий готовности

- Появился устойчивый non-Telegram service layer.
- Handlers и jobs теперь можно выносить без circular imports.

## PR 4. Вынести `matplotlib` в `charts.py`

### Цель

Сконцентрировать всё, что связано с chart rendering, в одном модуле и убрать `matplotlib` из entrypoint-модуля.

### Что переносим

- `CHART_COLORS`
- tick/axis/style helpers:
  - `format_month_short_label`
  - `format_day_month_label`
  - `build_month_tick_labels`
  - `pick_tick_indices`
  - `build_date_ticks`
  - `rub_axis_formatter`
  - `pct_axis_formatter`
  - `set_chart_header`
  - `apply_chart_style`
  - `annotate_series_last_point`
  - `annotate_bar_values`
  - `set_value_axis_limits`
- chart builders:
  - `build_history_chart`
  - `build_year_chart`
  - `build_year_monthly_delta_chart`
  - `render_twr_chart`

### Инструкции

1. Создать `src/bot/charts.py`.
2. Перенести туда `matplotlib` imports и `matplotlib.use("Agg")`.
3. Все chart builders должны зависеть только от:
   - `runtime.py`
   - `queries.py`
   - `services.py` при необходимости
4. В `bot.py` больше не должно быть прямого `matplotlib` import.
5. Убедиться, что chart API не меняет сигнатуры, которые уже использует `cmd_year`, `cmd_history`, `cmd_twr`.

### Критерий готовности

- Любая работа с `matplotlib` находится только в `charts.py`.
- Контейнерная сборка `docker compose build bot` проходит.

## PR 5. Вынести dataset/export в `dataset.py`

### Цель

Изолировать ZIP/export pipeline и dataset-specific helpers, чтобы `/dataset` перестал тянуть на себе половину монолита.

### Что переносим

- dataset-specific helpers:
  - `classify_operation_group`
  - `is_income_event_backed_tax_operation`
  - `build_logical_asset_id`
  - `build_asset_alias_lookup`
  - `build_reconciliation_by_asset_type`
- dataset builders:
  - `build_dataset_export`
  - `build_dataset_readme`
  - `create_dataset_archive`

### Инструкции

1. Создать `src/bot/dataset.py`.
2. Перенести туда dataset-specific код без изменения формата архива и файлов:
   - `dataset.json`
   - `README_AI.md`
   - `daily_timeseries.csv`
   - `positions_current.csv`
   - `operations.csv`
   - `income_events.csv`
3. В `services.py` и `queries.py` оставить только то, что действительно общее.
4. Обновить [`tests/test_dataset_helpers.py`](/Users/andrew/Dev/FinanceTracker/tests/test_dataset_helpers.py), чтобы он читал symbols из `dataset.py`, а не из `bot.py`.
5. В `bot.py` оставить только вызов `create_dataset_archive(...)` из нового модуля.

### Критерий готовности

- `/dataset` по-прежнему выдаёт тот же архивный контракт.
- Dataset-specific helpers больше не лежат в `bot.py`.

## PR 6. Вынести scheduled jobs в `jobs.py`

### Цель

Сделать отдельный модуль для periodic tasks и notification loops, чтобы фоновые джобы не жили рядом с Telegram handlers.

### Что переносим

- `jobqueue_smoke_test_job`
- `daily_job`
- `check_income_events`
- `check_invest_notifications`
- новый wiring helper: `register_jobs(app)`

### Инструкции

1. Создать `src/bot/jobs.py`.
2. Перенести туда все JobQueue coroutine’ы.
3. В `queries.py` заранее иметь всё, что нужно jobs для БД:
   - чтение unnotified income events
   - mark-as-notified
   - invest notification persistence
4. В `jobs.py` не оставлять raw SQL, кроме уже вынесенных query helper calls.
5. `register_jobs(app)` должен:
   - проверять `app.job_queue is not None`
   - регистрировать `run_daily(...)`
   - регистрировать `run_repeating(...)`
   - регистрировать `run_once(...)` для smoke-test при включённом флаге
6. Обновить [`tests/test_runtime_logging_guardrails.py`](/Users/andrew/Dev/FinanceTracker/tests/test_runtime_logging_guardrails.py): добавить `jobs.py` в `RUNTIME_FILES`.
7. Если нужно, добавить лёгкий test на wiring, например `tests/test_jobs_wiring.py`.

### Критерий готовности

- `bot.py` больше не содержит periodic jobs.
- Job scheduling инкапсулирован в `register_jobs(app)`.

## PR 7. Вынести Telegram handlers в `handlers.py`

### Цель

Отделить transport layer Telegram от business/service layer.

### Что переносим

- `debug_command_probe`
- все `cmd_*`
- новый wiring helper: `register_handlers(app)`

### Инструкции

1. Создать `src/bot/handlers.py`.
2. Перенести туда все Telegram command handlers.
3. Оставить в handlers только orchestration:
   - авторизация
   - разбор аргументов
   - вызов services/charts/dataset
   - отправка сообщений/файлов
4. В handlers не должно быть:
   - raw SQL
   - сложной финансовой математики
   - `matplotlib`
5. `register_handlers(app)` должен зарегистрировать:
   - `MessageHandler(filters.COMMAND, debug_command_probe)`
   - все `CommandHandler(...)`
6. Обновить [`tests/test_runtime_logging_guardrails.py`](/Users/andrew/Dev/FinanceTracker/tests/test_runtime_logging_guardrails.py): добавить `handlers.py` в `RUNTIME_FILES`.
7. Если нужно, добавить лёгкий test на wiring, например `tests/test_handlers_wiring.py`, который проверяет список зарегистрированных команд.

### Критерий готовности

- `bot.py` больше не содержит `cmd_*`.
- Telegram-specific логика изолирована в `handlers.py`.

## PR 8. Добить распил и сделать `bot.py` тонким composition root

### Цель

Завершить распил: `bot.py` должен остаться тонким entrypoint-модулем без бизнес-логики.

### Что должно остаться в `bot.py`

- импорт `TELEGRAM_BOT_TOKEN`, `logger`, `HOST_TZ` или других bootstrap-only вещей из `runtime.py`
- `main()`
- создание `Application`
- вызовы `register_handlers(app)` и `register_jobs(app)`
- startup log
- `app.run_polling()`
- `if __name__ == "__main__": ...`

### Инструкции

1. Удалить из `bot.py` всё, что уже живёт в `runtime.py`, `queries.py`, `services.py`, `charts.py`, `dataset.py`, `jobs.py`, `handlers.py`.
2. Убедиться, что `bot.py` больше не используется как склад compatibility re-export’ов для тестов.
3. Привести AST-based tests к новым модулям окончательно.
4. Обновить [`docs/ARCHITECTURE.md`](/Users/andrew/Dev/FinanceTracker/docs/ARCHITECTURE.md):
   - зафиксировать новую модульную структуру бота
   - кратко описать границы модулей
5. При необходимости кратко обновить [`README.md`](/Users/andrew/Dev/FinanceTracker/README.md), если там появятся упоминания внутренней структуры разработки.

### Критерий готовности

- `bot.py` стал тонким composition root.
- Внутренняя структура бота читается по модулям, а не по секциям одного файла.
- Все тесты и пробная сборка проходят.

## Что не делать внутри этого плана

- Не менять публичные команды и их UX ради “чистоты”.
- Не переписывать SQL ради “красоты”, если тот же SQL уже покрыт рабочим поведением.
- Не объединять несколько PR шагов в один большой PR.
- Не делать параллельно feature work внутри этих PR.

## Оптимальный порядок коммитов и сборки

Для каждого PR:

1. Сделать только один шаг из этого плана.
2. Прогнать:

```bash
python3 -m compileall src
python3 -m unittest discover -s tests -p "test_*.py"
docker compose config > /dev/null
```

3. Зафиксировать коммит.
4. Прогнать пробную сборку:

```bash
docker compose build bot
```

5. Только после этого переходить к следующему PR.

