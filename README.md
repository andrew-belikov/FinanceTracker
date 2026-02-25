# FinanceTracker

Трекер портфеля (T‑Invest / T‑Bank Invest API) + Telegram‑бот с отчётами и уведомлениями.

## Возможности

- Сохраняет снапшоты портфеля в Postgres с заданным интервалом.
- Telegram‑бот:
  - ежедневное уведомление в заданное время;
  - недельный отчёт по пятницам;
  - месячный отчёт в последний день месяца;
  - уведомление о новом максимуме портфеля «по итогу дня».
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


## Как проверить, что патч работает и данные пишутся (PowerShell)

Запускать из `D:\FinanceTracker>`:

```powershell
# 1) Поднять/перезапустить сервисы
cd D:\FinanceTracker
docker compose up -d --build --force-recreate --remove-orphans

# 2) Убедиться, что контейнеры в статусе Up
docker compose ps

# 3) Проверить логи tracker (ищем operations_sync и отсутствие operations_sync_failed)
# Иногда сразу после старта логов мало: можно смотреть в follow-режиме 1-2 минуты.
docker compose logs --tail=200 tracker
docker compose logs -f tracker

# 4) Проверить, что в operations появляются записи
# Вариант A: краткий числовой вывод
docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -tA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM operations;"'

# Вариант B (диагностический): явный префикс, если в консоли "пусто"
docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT ''operations_total='' || COUNT(*) FROM operations;"'

docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS amount_sum FROM operations GROUP BY operation_type ORDER BY cnt DESC;"'

# 5) Проверить последние операции
docker compose exec -T db sh -lc 'psql -X -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT date, operation_type, amount, currency, description FROM operations ORDER BY date DESC LIMIT 20;"'

# 6) Проверить обратную совместимость представления deposits
docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -tA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM deposits;"'
# Диагностический вариант с префиксом

docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT ''deposits_rows='' || COUNT(*) FROM deposits;"'
docker compose exec -T db sh -lc 'psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT date, amount, currency, description FROM deposits ORDER BY date DESC LIMIT 20;"'
```


Если видите ошибки PowerShell вида `Имя "*" не распознано`/`Unterminated quoted string` или `FATAL: role "-d" does not exist`, проблема в хостовом парсинге кавычек/переменных. Команды выше используют одинарные кавычки вокруг `sh -lc` и берут `$POSTGRES_USER/$POSTGRES_DB` уже **внутри контейнера db**, что снимает эти проблемы.

Ожидаемый результат:
- в логах `tracker` есть событие `operations_sync`;
- команда `SELECT COUNT(*) FROM operations;` возвращает число (желательно `> 0`);
- команда `SELECT COUNT(*) FROM deposits;` возвращает число (для активного счёта обычно `> 0`);
- таблицы с последними строками (`ORDER BY date DESC LIMIT 20`) показывают реальные записи.
- если `COUNT(*)` выглядит пустым, используйте диагностические команды с префиксом `operations_total=` / `deposits_rows=`.

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
- создаёт view `deposits` для обратной совместимости старых SQL-запросов;
- не копирует данные напрямую из `deposits_legacy`: исторические операции догружаются tracker-сервисом из API.

Применение вручную:

```powershell
Get-Content .\migrations\20260221_operations_from_deposits.sql | docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB
```

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
docker compose exec -T db sh -lc "psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -c \"SELECT COUNT(*) AS deposits_rows FROM deposits;\""
docker compose exec -T db psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT date, amount, currency, description, source FROM deposits ORDER BY date DESC LIMIT 10;"
```

5) Запустить сервисы обратно и дать tracker догрузить историю операций из API:

```powershell
docker compose up -d tracker bot
docker compose logs --tail=200 tracker
```

6) После стабилизации проверить, что в `operations` появились операции и бот продолжает читать `deposits`:

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

## Конфигурация

Список переменных окружения — в `.env.example` и в `docs/CONFIG.md`.

Ежедневный JobQueue в боте теперь также делает health-check данных:
- проверяет актуальность `portfolio_snapshots` и предупреждает при отставании больше 1 дня;
- выполняет sanity-check по `deposits` (совместимое view на `operations`) и сообщает, если таблица пустая.

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
