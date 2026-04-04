# Runbook FinanceTracker

Все команды ниже предполагают запуск из корня репозитория, где лежит основной [`compose.yml`](../compose.yml).
Примеры используют POSIX shell и описывают единственный поддерживаемый workflow: `docker compose` из корня репозитория.

## Базовые Принципы

- Основной Docker entrypoint это корневой `compose.yml`.
- `docker compose config` требует существующий локальный `.env`, потому что в сервисах используется `env_file: .env`.
- Не используйте `docker compose down -v`, если хотите сохранить данные Postgres.

## Первый Запуск

1. Создайте `.env`:

```bash
cp .env.example .env
```

2. Создайте внешний volume:

```bash
docker volume create financetracker_fintracker-db
```

3. Поднимите стек:

```bash
docker compose up -d --build
```

При создании контейнера `tracker` entrypoint автоматически устанавливает сертификаты из `docker/certs/` в системную trust store.

4. Проверьте статус:

```bash
docker compose ps
```

5. Проверьте логи:

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

## Обновление Без Изменения Схемы

```bash
git pull --ff-only
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Если менялись только файлы в `docker/certs/`, достаточно пересоздать `tracker`:

```bash
docker compose up -d --force-recreate tracker
```

## Backup И Restore

### Backup

```bash
docker compose exec -T db pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > backup.sql
```

### Restore

```bash
cat backup.sql | docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Перед любой миграцией или крупным redeploy делайте backup.

## Когда Нужны SQL-Миграции

На чистой БД проект запускается и без них, но исторические SQL-скрипты нужны, если:

- вы обновляете старую БД со схемой `deposits`;
- вам нужен точный migrated shape таблицы `operations`;
- в текущей БД ещё нет таблицы `income_events` или дополнительных полей из поздних миграций.

## Миграции В Репозитории

| Файл | Когда применять | Что меняет | Важные caveats |
| --- | --- | --- | --- |
| `migrations/20260221_operations_from_deposits.sql` | при переходе со старой схемы `deposits` | создаёт `operations`, переименовывает старую `deposits` в `deposits_legacy`, создаёт исторический compatibility-view `deposits` | активный runtime-код читает пополнения из `operations`, а не из `deposits` |
| `migrations/20260221_operations_from_deposits.rollback.sql` | только если нужен частичный rollback исторического compatibility-слоя | возвращает `deposits` как таблицу, если есть `deposits_legacy` | не удаляет `operations` |
| `migrations/20260225_operations_add_instrument_columns.sql` | если существующая `operations` ещё без `instrument_uid` и `figi` | добавляет 2 колонки | после применения `tracker` дозаполняет пустые поля на последующих синках |
| `migrations/20260226_income_events.sql` | если существующая схема ещё без `income_events` | создаёт таблицу событий дохода | без этой таблицы минутные income-notifications и часть отчётных сумм не работают |
| `migrations/20260304_operations_operation_item_fields.sql` | если существующая `operations` ещё без расширенных полей `OperationItem` | добавляет колонки `state`, `commission`, `yield`, `instrument_type` и другие | также добавляет unique-констрейнт по `operation_id`; после миграции возможен backfill исторических строк |

## Рекомендуемый Порядок Миграции

1. Если миграция затрагивает рабочие сервисы, остановите writer и reader:

```bash
docker compose stop tracker bot
```

2. Сделайте backup:

```bash
docker compose exec -T db pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > pre_migration_backup.sql
```

3. Примените только нужные SQL-скрипты:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260221_operations_from_deposits.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260225_operations_add_instrument_columns.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260226_income_events.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260304_operations_operation_item_fields.sql
```

4. Поднимите сервисы обратно:

```bash
docker compose up -d tracker bot
```

5. Проверьте логи:

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

## SQL-Проверки После Миграции

Проверить таблицы, которые реально должны появиться после выбранных миграций:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d operations"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d income_events"
```

Если вы обновляли именно старую схему `deposits`, можно дополнительно проверить исторический compatibility-view:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d deposits"
```

Проверить наличие операций и последних пополнений:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT MAX(date)::date AS latest_input_date FROM operations WHERE operation_type='OPERATION_TYPE_INPUT';"
```

Проверить свежие снапшоты:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT snapshot_date, snapshot_at, total_value FROM portfolio_snapshots ORDER BY snapshot_date DESC, snapshot_at DESC LIMIT 10;"
```

Проверить события дохода:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, figi, event_date, event_type, net_amount, net_yield_pct, notified FROM income_events ORDER BY created_at DESC LIMIT 20;"
```

## Telegram Smoke-Check

После старта или обновления вручную проверьте:

- `/today`
- `/week`
- `/month`
- `/year`
- `/history`
- `/structure`
- `/twr`

Если нужна принудительная проверка JobQueue, временно включите:

```env
JOBQUEUE_SMOKE_TEST_ON_START=true
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS=20
```

После рестарта `bot` он отправит тестовое сообщение в каждый `TARGET_CHAT_IDS`.

## Типовые Диагностические Сценарии

### `tracker` не пишет новые снапшоты

Проверьте:

```bash
docker compose logs --tail=200 tracker
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT snapshot_date, snapshot_at, total_value FROM portfolio_snapshots ORDER BY snapshot_date DESC, snapshot_at DESC LIMIT 5;"
```

Что искать:

- ошибки `TINVEST_API_TOKEN`;
- ошибки HTTP/SSL;
- проблемы с выбором счёта;
- отсутствие новых строк в `portfolio_snapshots`.

### `bot` не отправляет недельный или месячный отчёт

Проверьте:

```bash
docker compose logs --tail=200 bot
```

Что учитывать:

- JobQueue у бота живёт в локальном времени контейнера;
- `TIMEZONE` не переводит ежедневный запуск на новое civil time;
- если контейнер живёт в UTC, событие `18:00` тоже будет UTC.

### Нет уведомлений о купонах или дивидендах

Проверьте:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, figi, event_type, net_amount, notified FROM income_events ORDER BY created_at DESC LIMIT 20;"
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Что учитывать:

- `tracker` должен сначала создать строки в `income_events`;
- `bot` помечает строку как `notified=true` только после успешной отправки во все целевые чаты;
- если таблицы `income_events` нет, этот механизм не работает.

### `/year` не совпадает с `/month` или `/history`

Это не обязательно ошибка. Команды читают данные по-разному:

- `/year` использует dedup и только `OPERATION_STATE_EXECUTED`;
- `/month`, `/week`, `/today`, `/history` и trigger-ветки используют raw `operations`.

Сначала проверьте, нет ли дубликатов или неисполненных строк:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_id, COUNT(*) FROM operations GROUP BY operation_id HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT 20;"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT state, COUNT(*) FROM operations GROUP BY state ORDER BY state;"
```

## Проверки Репозитория

Доступная автоматическая проверка:

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m compileall src
python3 -m unittest discover -s tests
docker compose config > /dev/null
```

Нюансы среды:

- `python -m compileall src` указан в `AGENTS.md`, но alias `python` может отсутствовать;
- прямой `python3 -m compileall src` в sandbox-среде может упираться в системный cache path и завершаться `PermissionError`, поэтому выше приведён безопасный вариант через `PYTHONPYCACHEPREFIX=/tmp/pycache`;
- `docker compose config` имеет смысл запускать только после создания реального `.env`.
