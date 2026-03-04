# FinanceTracker

Трекер портфеля (T‑Invest / T‑Bank Invest API) + Telegram‑бот с отчётами и уведомлениями.

## Возможности

- Сохраняет снапшоты портфеля в Postgres (режим interval или cron, в зависимости от `SNAPSHOT_MODE`).
- Telegram‑бот:
  - недельный отчёт по пятницам в 18:00 (по времени хоста);
  - в дневных/недельных/месячных отчётах показываются доходы (купоны, дивиденды) и расходы (комиссии, налоги);
  - месячный отчёт в последний день месяца в 18:00 (по времени хоста);
  - команда `/twr` с расчётом TWR (time-weighted return) и графиком по дням;
  - команда `/year [YYYY]`: без аргумента отчёт за текущий год (YTD), с аргументом — за календарный год, в сообщении показывается явный период и стоимость портфеля на конец периода; к отчёту прикладывается PNG с помесячными столбцами (внешний — стоимость портфеля на последний снапшот месяца, внутренний — пополнения за месяц);
  - для `/year` финансовые суммы считаются только по исполненным операциям (`state = OPERATION_STATE_EXECUTED`) c дедупликацией по `operation_id` (последняя запись по `id`), чтобы не было задвоений;
  - в `/year` добавлены блоки top-N по активам (с строкой `Итого`) для реализованного результата по продажам (`yield + commission`) и для net-доходов по купонам/дивидендам (gross + tax), а нереализованный результат берётся из последнего снапшота периода;
  - авто‑уведомления о зачислении купонов и дивидендов (по событиям из БД);
  - уведомление о новом максимуме портфеля «по итогу дня»;
  - уведомление о выполнении годового плана пополнений.
- Ограничение доступа по списку доверенных Telegram user_id.

## Архитектура

- `tracker` — сервис, который опрашивает Invest API и пишет снапшоты в БД.
- `bot` — Telegram‑бот, который читает данные из БД и отправляет отчёты.
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
- `/year` — приходит summary за год, PNG-график с помесячными столбцами (стоимость портфеля и пополнения) и блок изменений позиций (по умолчанию текущий год YTD).
- `/year YYYY` — аналогичный отчёт за указанный календарный год.
- `/history` — строится график стоимости и пополнений (без ошибок отправки файла).
- `/structure` — корректно отрисовывается структура портфеля.
- `/twr` — приходит метрика TWR и график по дням.

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
