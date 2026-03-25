# FinanceTracker

Трекер портфеля (T‑Invest / T‑Bank Invest API) + Telegram‑бот с отчётами и уведомлениями.

## Возможности

- Сохраняет снапшоты портфеля в Postgres (режим interval или cron, в зависимости от `SNAPSHOT_MODE`).
- Telegram‑бот:
  - недельный отчёт по пятницам в 18:00 (по времени хоста);
  - в дневных/недельных/месячных отчётах показываются доходы (купоны, дивиденды) и расходы (комиссии, налоги);
  - месячный отчёт в последний день месяца в 18:00 (по времени хоста);
  - команда `/targets` показывает текущие таргеты аллокации, а `/targets set stocks=50 bonds=30 cash=20` сохраняет целевые доли по классам активов;
  - команда `/rebalance` показывает текущие отклонения от таргетов и расчёт buy/sell по классам, чтобы вернуться к целевой структуре;
  - команда `/invest <sum>` подсказывает, как распределить новое пополнение по таргетам;
  - команда `/twr` с corrected дневным period-first TWR по активному счёту, XIRR в годовых, run-rate на 31 декабря (без новых внешних cashflow) и графиком по дням в двух связанных панелях: стоимость портфеля и TWR;
  - команда `/dataset` отправляет один ZIP-архив для AI-анализа: внутри `dataset.json`, `daily_timeseries.csv`, `positions_current.csv`, `operations.csv`, `income_events.csv` и `README_AI.md`; summary в архиве period-first, а не lifetime-first, плюс есть `logical_asset_id`, `reconciliation_*` и quality flags;
  - команда `/year` работает в двух режимах: без аргумента — отчёт за текущий год (YTD), с аргументом `/year YYYY` — отчёт за указанный календарный год;
  - `/year` отправляет 4 сообщения: summary, PNG-график с двумя связанными блоками (сверху — стоимость портфеля на конец месяца, снизу — пополнения за месяц), дополнительный PNG-график `результат по месяцам` (дельта без учёта пополнений: изменение стоимости минус пополнения за период месяца), и movements (сгруппированный блок изменений позиций 🆕/📈/📉/✅ по первому и последнему снапшоту внутри периода, с агрегацией по логическому активу: ticker → name → figi; валютные позиции из qty-движений исключаются);
  - для `/year` финансовые суммы считаются только по исполненным операциям (`state = OPERATION_STATE_EXECUTED`) c дедупликацией по `operation_id` (последняя запись по `id`), чтобы не было задвоений;
  - в `/year` добавлены блоки top-N по активам (с строкой `Итого`) и ключевые формулы: realized net по продажам = `yield + commission`, income net по купонам/дивидендам = `gross + tax`, unrealized на конец периода берётся из последнего снапшота периода;
  - авто‑уведомления о зачислении купонов и дивидендов (по событиям из БД);
  - авто‑подсказка по каждому новому пополнению счёта: бот автоматически отправляет рекомендацию в логике `/invest <сумма>`;
  - уведомление о новом максимуме портфеля «по итогу дня»;
  - уведомление о выполнении годового плана пополнений.
- Ограничение доступа по списку доверенных Telegram user_id.

## Гайдлайн по графикам

- Один график отвечает на один главный вопрос; если вопросов два, лучше разделить на две связанные панели.
- Главная серия должна быть самой контрастной, вторичные серии спокойнее и тоньше.
- Вместо перегруженной легенды предпочтительны прямые подписи на последних значениях и на ключевых экстремумах.
- Подписи и оси должны использовать короткий человекочитаемый формат: `янв`, `фев`, `1.2 млн ₽`, `+4 %`.
- Положительная динамика всегда показывается одним семейством цветов, отрицательная — другим; нейтральные состояния не должны спорить с акцентами.
- Сетка остаётся только вспомогательной: мягкая по оси Y, без лишнего шума по оси X.
- Если данные разных масштабов мешают чтению, их нужно разводить по отдельным панелям, а не уплотнять в одну ось.
- График в Telegram должен быть понятен без зума: крупные акценты, минимум декоративных деталей, не больше 2-3 основных цветов.

## Архитектура

- `tracker` — сервис, который опрашивает Invest API и пишет снапшоты в БД.
- `bot` — Telegram‑бот, который читает данные из БД, хранит таргеты аллокации и отправляет отчёты.
- `db` — Postgres.

```
Postgres  <--- tracker (snapshots)
   ^
   |
  bot (reports)
```

## Быстрый старт (Windows, Docker Desktop)

1) Клонируйте репозиторий и перейдите в папку проекта.

2) Создайте файл окружения:

```powershell
Copy-Item .\.env.example .\.env
notepad .\.env
```

Заполните минимум:
- `TELEGRAM_BOT_TOKEN`
- `TINVEST_API_TOKEN`
- `ALLOWED_USER_IDS`
- `POSTGRES_PASSWORD`

3) (Опционально) создайте внешний volume для Postgres.

Проект по умолчанию использует внешний volume `financetracker_fintracker-db` (удобно, чтобы данные переживали пересборки).

```powershell
docker volume create financetracker_fintracker-db
```

4) Запустите стек:

```powershell
docker compose up -d --build
```

Проверка статуса и логов:

```powershell
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```

### Опциональный outbound proxy только для `bot`

Если на сервере прямой доступ к Telegram нестабилен, можно включить VLESS+Reality только для контейнера `bot`.
`tracker` и `db` при этом продолжают работать напрямую, без proxy.

Добавьте в `.env`:

```env
BOT_PROXY_ENABLED=true
BOT_VLESS_URL="vless://uuid@host:443?encryption=none&security=reality&sni=example.com&fp=chrome&pbk=PUBLIC_KEY&sid=SHORT_ID&type=tcp#bot"
```

Чтобы выключить режим, достаточно вернуть:

```env
BOT_PROXY_ENABLED=false
BOT_VLESS_URL=""
```

После изменения `.env` пересоберите и перезапустите стек:

```powershell
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps
docker compose logs --tail=200 xray-client
docker compose logs --tail=200 bot
```

Быстрый smoke-test маршрута `bot -> xray-client -> Telegram`:

```powershell
docker compose exec bot python proxy_smoke.py
```

Сценарий проверки:
- при `BOT_PROXY_ENABLED=true` `docker compose ps` показывает `xray-client` в состоянии `healthy`;
- при `BOT_PROXY_ENABLED=false` `xray-client` не попадает в обычный `docker compose ps`, потому что proxy не активен; при необходимости детальный статус виден в `docker compose ps -a xray-client` как `Exited (0)`;
- все runtime-логи `bot`, `tracker`, `xray-client`, startup smoke и healthcheck идут как JSON Lines в `stdout`; удобнее всего смотреть их через `docker compose logs ...`;
- в логах `xray-client` ищите события `xray_proxy_ready`, `xray_telegram_smoke_completed` и `xray_process_output`;
- `proxy_smoke.py` внутри `bot` пишет одно структурированное событие `bot_startup_smoke_completed` или `bot_startup_smoke_failed` и подтверждает доступность Telegram API и прямой TCP-доступ к `db`;
- при `BOT_PROXY_ENABLED=false` `xray-client` пишет событие `xray_proxy_disabled`, а `tracker` продолжает работать как раньше.

## Обновление (пересборка без потери данных)

```powershell
docker compose up -d --build --force-recreate --remove-orphans
```

## Деплой с перезапуском всех сервисов без потери данных

Надёжный порядок (перезапуск всех сервисов + сохранность БД):

```powershell
# 1) (Опционально, но рекомендуется) бэкап текущей БД
docker compose exec -T db pg_dump -U $env:POSTGRES_USER $env:POSTGRES_DB > backup_before_redeploy.sql

# 2) Обновить код
# (внутри репозитория)
git pull --ff-only

# 3) Пересобрать и перезапустить весь стек
# ВАЖНО: не использовать down -v (это удалит volume с данными)
docker compose up -d --build --force-recreate --remove-orphans

# 4) Проверить, что все сервисы поднялись
docker compose ps

# 5) Проверить логи приложений
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Критично для сохранности данных:
- данные Postgres живут в Docker volume `financetracker_fintracker-db`;
- команда `docker compose down` сама по себе данные не удаляет;
- **не запускать** `docker compose down -v`, если нужна сохранность БД.


## Данные и бэкап

Данные Postgres хранятся в Docker volume `financetracker_fintracker-db`. Не выполняйте `docker compose down -v`, если не хотите удалить БД.

Пример дампа:

```powershell
docker exec -i financetracker-db-1 pg_dump -U $env:POSTGRES_USER $env:POSTGRES_DB > backup.sql
```


## Подготовка к AI coding

- Базовые правила для AI-агентов: `AGENTS.md`.
- Требования к PR и проверкам: `CONTRIBUTING.md`.

Перед коммитом рекомендуется выполнить:

```bash
python -m compileall src
docker compose config
```


## SQL-миграции

В репозитории есть SQL-миграция `migrations/20260221_operations_from_deposits.sql`.

Она:
- создаёт таблицу `operations`;
- сохраняет старую таблицу как `deposits_legacy`;
- создаёт view `deposits` только для legacy/backward compatibility старых SQL-запросов;
- новый код должен читать пополнения из `operations` (с фильтром по `operation_type`), а не из `deposits`;
- не копирует данные напрямую из `deposits_legacy`: исторические операции догружаются tracker-сервисом из API.

Применение вручную:

```powershell
Get-Content .\migrations\20260221_operations_from_deposits.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

### Коротко: миграция на новую схему

```powershell
# 1) Остановить writer/reader, чтобы зафиксировать состояние на время миграции
docker compose stop tracker bot

# 2) Применить SQL-миграцию
Get-Content .\migrations\20260221_operations_from_deposits.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB

# 3) Поднять сервисы обратно
docker compose up -d tracker bot

# 4) Проверить результат миграции
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT COUNT(*) AS deposits_rows FROM deposits;"
```

Ожидаемый результат: данные операций и пополнений читаются из `operations`; view `deposits` остаётся только для обратной совместимости legacy-запросов.

### Как применить миграцию без потери данных (рекомендуемый порядок)

1) Остановить запись в БД со стороны приложений (короткое окно на миграцию):

```powershell
docker compose stop tracker bot
```

2) Сделать бэкап БД перед изменениями:

```powershell
docker compose exec -T db pg_dump -U $env:POSTGRES_USER $env:POSTGRES_DB > pre_operations_migration_backup.sql
```

3) Применить миграцию:

```powershell
Get-Content .\migrations\20260221_operations_from_deposits.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

4) Проверить, что совместимость сохранена (данные по пополнениям читаются через `deposits`):

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT COUNT(*) AS operations_input FROM operations WHERE operation_type='OPERATION_TYPE_INPUT';"
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT COUNT(*) AS deposits_rows FROM deposits;"
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT date, amount, currency, description, source FROM deposits ORDER BY date DESC LIMIT 10;"
```

5) Запустить сервисы обратно и дать tracker догрузить историю операций из API:

```powershell
docker compose up -d tracker bot
docker compose logs --tail=200 tracker
```

6) После стабилизации проверить, что в `operations` появились операции и бот читает пополнения напрямую из `operations` (с фильтром по `operation_type`):

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose logs --tail=200 bot
```

Откат (с ограничениями) — `migrations/20260221_operations_from_deposits.rollback.sql`:

- view `deposits` удаляется;
- если есть `deposits_legacy`, она возвращается как таблица `deposits`;
- таблица `operations` не удаляется.

```powershell
Get-Content .\migrations\20260221_operations_from_deposits.rollback.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

Дополнительная миграция `migrations/20260225_operations_add_instrument_columns.sql` добавляет в `operations`
поля `instrument_uid` и `figi`.

```powershell
Get-Content .\migrations\20260225_operations_add_instrument_columns.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```


Миграция `migrations/20260304_operations_operation_item_fields.sql` расширяет таблицу `operations` полями
из `OperationItem` (кроме `trades_info.trades`) и добавляет уникальность по `operation_id`
для идемпотентного upsert.

```powershell
Get-Content .\migrations\20260304_operations_operation_item_fields.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

Синхронизация операций теперь использует `OperationsService/GetOperationsByCursor` с `withoutTrades=true`
и постраничной обработкой `nextCursor`.
Если после миграции у старых записей новые поля операции пустые, tracker автоматически
делает backfill с даты открытия счёта и затем возвращается к инкрементальному режиму.

Миграция `migrations/20260226_income_events.sql` добавляет таблицу `income_events`.

Новая схема уведомлений:
- `tracker` фиксирует события купонов/дивидендов в `income_events`;
- `bot` раз в минуту читает `income_events.notified = false`, отправляет уведомления и помечает событие как отправленное.

```powershell
Get-Content .\migrations\20260226_income_events.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

Миграция `migrations/20260324_dataset_source_fields.sql` добавляет поля в `portfolio_positions`
для source-aware `/dataset` и создаёт таблицу `asset_aliases`.

Она нужна, чтобы:
- `positions_current` экспортировал `asset_uid`, `instrument_uid`, `position_uid`, `logical_asset_id`;
- `/dataset` мог строить reconciliation по классам активов из snapshot totals;
- смена FIGI не ломала склейку по логическому активу.

```powershell
Get-Content .\migrations\20260324_dataset_source_fields.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

Миграция `migrations/20260324_rebalance_targets_and_invest_notifications.sql` добавляет таблицы
`rebalance_targets` и `invest_notifications` для `/targets`, `/rebalance`, `/invest`
и автоматических подсказок по новым пополнениям.

```powershell
Get-Content .\migrations\20260324_rebalance_targets_and_invest_notifications.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

Важно: эту миграцию нужно применить **до** пересборки `tracker`, иначе новый код tracker
не сможет записывать расширенные поля `portfolio_positions`.

Если после переноса backup с Windows в `operations.description` появилась битая кириллица,
можно выполнить разовый repair внутри контейнера `tracker`:

```powershell
docker compose exec -T tracker python repair_operations_description_encoding.py
```

Для временного переключения между ветками есть helper-скрипт
`scripts/migrate_branch_switch.ps1`.

Он:
- находит новые SQL-файлы в `migrations/` для целевой ветки через `git diff FROM...TO`;
- поднимает `db`, останавливает `tracker`/`bot`, применяет найденные миграции;
- выполняет `compileall`, `docker compose config` и `unittest`;
- пересобирает и поднимает контейнеры, затем показывает статус и последние логи.

Примеры:

```powershell
.\scripts\migrate_branch_switch.ps1 -FromBranch main -ToBranch dev
.\scripts\migrate_branch_switch.ps1 -FromBranch dev -ToBranch main
```

Запуск из Windows:

```powershell
cd C:\path\to\FinanceTracker
powershell -ExecutionPolicy Bypass -File .\scripts\migrate_branch_switch.ps1 -FromBranch main -ToBranch dev
```

Если вы уже открыли PowerShell в корне репозитория:

```powershell
.\scripts\migrate_branch_switch.ps1 -FromBranch main -ToBranch dev
```

Если Windows блокирует запуск `.ps1`, можно разово разрешить выполнение для текущего окна:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\migrate_branch_switch.ps1 -FromBranch main -ToBranch dev
```

### Пошагово: деплой + миграция + проверка (Windows Terminal / PowerShell)

1) Откройте Windows Terminal (PowerShell) в папке репозитория и обновите код:

```powershell
git pull --ff-only
```

2) Остановите сервисы `tracker` и `bot` на время миграции (БД остаётся запущенной):

```powershell
docker compose stop tracker bot
```

3) Примените миграцию `income_events`:

```powershell
Get-Content .\migrations\20260226_income_events.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

4) Пересоберите и поднимите весь стек:

```powershell
docker compose up -d --build --force-recreate --remove-orphans
```

5) Проверьте статус контейнеров:

```powershell
docker compose ps
```

6) Проверьте, что таблица создана:

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "\d income_events"
```

7) Проверьте, что tracker создаёт события, а bot отправляет уведомления:

```powershell
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Ожидаемо:
- в логах tracker появляются записи `income_event_created`;
- в логах bot появляются `income_event_notification_sent`.

8) Быстрая проверка вручную (тестовое событие в БД):

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "INSERT INTO income_events (account_id, figi, event_date, event_type, gross_amount, tax_amount, net_amount, net_yield_pct) VALUES ('manual-check', 'TESTFIGI', CURRENT_DATE, 'dividend', 100.00, -13.00, 87.00, 1.23) ON CONFLICT DO NOTHING;"
docker compose logs --tail=200 bot
```

9) Проверка, что событие пометилось как отправленное:

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT id, account_id, figi, event_type, net_amount, net_yield_pct, notified, created_at FROM income_events ORDER BY created_at DESC LIMIT 20;"
```

10) Если нужно откатить только миграцию `income_events`, удалите таблицу вручную (осторожно, удалит данные событий):

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "DROP TABLE IF EXISTS income_events;"
```

После применения tracker при очередной синхронизации:
- заполняет новые поля для новых операций;
- делает backfill для уже существующих операций (обновляет строки, где `instrument_uid`/`figi` ещё `NULL`).

## Конфигурация

Список переменных окружения — в `.env.example` и в `docs/CONFIG.md`.

Авто-рассылки JobQueue:
- каждый день в 18:00 (по времени хоста) проверяются триггеры: новый максимум и выполнение годового плана;
- по пятницам в 18:00 дополнительно отправляется недельный отчёт;
- в последний день месяца в 18:00 дополнительно отправляется месячный отчёт.


## Чек-лист проверки бота после обновления

Ниже — минимальный практический smoke-check, чтобы убедиться, что обновление прошло корректно.

1) Проверить, что контейнеры запущены:

```powershell
docker compose ps
```

2) Проверить логи на старте (без traceback/DB errors):

```powershell
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```

3) Проверить данные операций в БД (по умолчанию бот считает пополнениями `OPERATION_TYPE_INPUT`):

```powershell
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT MAX(date)::date AS latest_input_date FROM operations WHERE operation_type='OPERATION_TYPE_INPUT';"
```

4) В Telegram вручную проверить команды бота:

- `/today` — приходит сводка и корректный PnL относительно суммы пополнений.
- `/week` — динамика считается относительно последнего снапшота до начала текущей рабочей недели (понедельник–пятница), плюс суммы за период.
- `/month` — динамика считается относительно последнего снапшота до начала месяца; также есть сумма пополнений за месяц и прогресс годового плана.
- `/year` — режим YTD (без аргумента): приходит summary за текущий год + 2 PNG-графика (основной и `прирост/падение по месяцам`) + movements (четвёртое сообщение с блоками изменений позиций по группам, агрегация в movements идёт по ticker с fallback на name/figi); график `прирост/падение по месяцам` считается без учёта пополнений (изменение стоимости минус пополнения за период месяца).
- `/year YYYY` — режим календарного года: аналогичный отчёт за указанный год (summary + 2 PNG-графика + movements).
- В summary `/year` формулы считаются так: realized net = `yield + commission`, income net = `gross + tax`, unrealized на конец периода — из последнего снапшота внутри выбранного периода.
- `/history` — строится график стоимости и пополнений (без ошибок отправки файла).
- `/structure` — корректно отрисовывается структура портфеля.
- `/twr` — приходит corrected дневной period-first TWR по активному счёту, XIRR в годовых, run-rate на 31 декабря без новых пополнений/выводов и график по дням.
- `/dataset` — приходит ZIP-архив с `json + csv + md`, пригодный для передачи ИИ-модели; внутри summary используются `period_*` поля, `has_full_history_from_zero`, `reconciliation_gap_abs`, а в `positions_current`/`operations` есть `logical_asset_id`.

## Бэклог `/dataset`

- Разложить `reconciliation_gap_abs` на именованные компоненты источника (`cash_free`, `cash_blocked`, `accrued_interest`, `other_assets_value`), если Invest API отдаёт их стабильно и однозначно.
- Довести source-aware reconciliation до полного совпадения не только по классам активов, но и по экономическому смыслу остатка внутри bond/cash-компонентов.
- Добавить честную lifetime-аналитику только после подтверждения полной истории портфеля с нуля и корректного cost basis по всем текущим позициям.
- Вынести lifetime-метрики в отдельный контракт датасета, чтобы не смешивать их с текущим period-first summary.
- Достроить strict cost basis / realized / unrealized analytics по логическому активу, а не только по текущему `figi`.
- Расширить alias-слой проверками и отчётами, чтобы смена FIGI автоматически подсвечивалась и не ломала downstream-аналитику.
- Добавить интеграционную проверку после миграции `20260324_dataset_source_fields.sql`: alias-группы заполняются, а `logical_asset_id` стабилен между операциями и текущими позициями.

5) Проверить авто-рассылки JobQueue:

- в логах `bot` есть событие старта (`Bot started. Daily job at 18:00 ...`) и запусков джоба;
- по пятницам приходит `/week`;
- в последний день месяца приходит `/month`;
- при новом максимуме/пересечении годового плана приходит соответствующий триггер.

Рекомендуется после деплоя выполнить команды из этого чек-листа и сохранить короткий отчёт: время проверки, какие команды Telegram протестированы, и результат.

## Безопасность

- Никогда не коммитьте `.env`.
- Если токены/пароли где‑то засветились — перевыпустите токены и смените пароли.

## Лицензия

MIT — см. `LICENSE`.

### Проверка JobQueue после запуска (опционально)

Чтобы один раз проверить, что JobQueue реально отправляет сообщения, включите в `.env`:

```env
JOBQUEUE_SMOKE_TEST_ON_START=true
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS=20
```

После старта контейнера `bot` в логе появятся события планирования и результата smoke-test.
