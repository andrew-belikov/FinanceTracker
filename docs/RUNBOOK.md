# Runbook FinanceTracker

Все команды ниже предполагают запуск из корня репозитория, где лежит основной [`compose.yml`](../compose.yml).
Примеры используют POSIX shell и описывают единственный поддерживаемый workflow: `docker compose` из корня репозитория.

## Базовые Принципы

- Основной Docker entrypoint это корневой `compose.yml`.
- По умолчанию сервисы читают `${APP_ENV_FILE:-.env}`. Для другого файла экспортируйте `APP_ENV_FILE` и передавайте `--env-file "$APP_ENV_FILE"` каждой Compose-команде.
- Не используйте `docker compose down -v`, если хотите сохранить данные Postgres.

## Первый Запуск

1. Создайте `.env` (или задайте другой путь в `APP_ENV_FILE`):

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

Сертификаты из `docker/certs/` устанавливаются в trust store во время сборки
образа `tracker`. Runtime-контейнеры работают без root и с read-only root filesystem; временные файлы создаются только в tmpfs `/tmp`.

4. Проверьте статус:

```bash
docker compose ps
```

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

## Обновление

Production deploy запускается автоматически после успешного workflow `CI` для
`push` в `main`. Workflow `Deploy FinanceTracker`:

1. создаёт временный чистый checkout точного SHA, прошедшего CI;
2. проверяет Compose-конфигурацию и выполняет pre-deploy backup с restore drill;
3. собирает образы с tag `git-<SHA>` и запускает Compose по зафиксированным image
   IDs;
4. ждёт healthchecks, запускает `migrate --check` и публикует source SHA и image
   IDs в GitHub Actions summary.

Не обновляйте production через `git pull` и ручной `docker compose build`: такой
запуск обходит связь между CI, source SHA и образами. После workflow проверьте
его summary, затем состояние и логи сервисов:

```bash
export APP_ENV_FILE=.env
docker compose --env-file "$APP_ENV_FILE" ps --all
docker compose --env-file "$APP_ENV_FILE" logs --tail=200 tracker bot reporter
```

Изменение `docker/certs/` также доставляется обычным exact-SHA deploy: сертификаты
встраиваются в образ `tracker` при сборке.

## Проверка логирования после deploy

После обновления проверьте JSON records за период обычной нагрузки. Штатные
Xray connection records должны иметь уровень `DEBUG`, а APScheduler не должен
выдавать минутные `auto_log` ниже `WARNING`:

```bash
docker compose logs --since=1h --no-log-prefix xray-client \
  | grep '"event": "xray_process_output"' \
  | grep -Ev '"level": "(DEBUG|WARNING|ERROR|CRITICAL)"' || true
docker compose logs --since=1h --no-log-prefix bot tracker \
  | grep '"logger": "apscheduler' \
  | grep -Ev '"level": "(WARNING|ERROR|CRITICAL)"' || true
```

Пустой вывод означает отсутствие штатного INFO-flood. Не фильтруйте
`WARNING`/`ERROR`: failover, recovery и ошибки child process должны оставаться
видимыми. Для exception records formatter обязан сохранять объект `error` с
`type`, `message`, `stack` и `where`; не отправляйте raw production logs в
тикеты или чаты без отдельной sanitization-проверки.

## Backup И Restore

### Backup перед deploy

`deploy.yml` запускает `scripts/predeploy_backup.sh` до candidate containers.
Скрипт создаёт no-clobber custom dump, migration ledger, manifest с source SHA и
CI run, SHA-256, затем восстанавливает dump во временную database, применяет к
ней migrations candidate-версии и выполняет `migrate --check`. Любая ошибка
блокирует deploy; production database при этом не меняется.

Задайте GitHub environment variable `FINANCETRACKER_BACKUP_DIR` на существующий
каталог вне checkout и Docker volumes. Храните копии не менее 30 дней и 10
последних успешных deploy; не удаляйте backup до нового успешного restore drill.

### Экстренный restore

```bash
docker compose exec -T db pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --clean --if-exists < /secure/path/postgres.dump
```

Production restore требует break-glass решения: остановить writers,
восстановить в новый volume/database, проверить `SHA256SUMS` и `migrate --check`,
после чего переключить services. Application rollback допустим только на
сохранённые previous image IDs и при подтверждённой schema compatibility.

## Миграции

Forward-миграции применяются автоматически сервисом `migrate` при каждом
`docker compose up`. Не запускайте отдельные SQL-файлы вручную и не изменяйте
уже применённые файлы: runner сверяет их SHA-256 с `schema_migrations`.

Автоматический порядок:

1. `db` проходит healthcheck.
2. Под advisory lock последовательно применяются ещё не зарегистрированные
   `migrations/*.sql`, кроме `*.rollback.sql`.
3. В одной транзакции с каждой миграцией в `schema_migrations` сохраняется
   имя файла и SHA-256.
4. Только после успешного завершения `migrate` запускаются `tracker`, `bot`
   и `reporter`.

Проверка без применения новых миграций:

```bash
docker compose run --rm migrate --check
docker compose logs --tail=200 migrate
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT filename, checksum_sha256, applied_at FROM schema_migrations ORDER BY filename;"
```

Если checksum уже применённого файла изменился либо файл удалён, runner
завершится ошибкой. Верните исходный файл и оформите изменение новой
forward-миграцией. Rollback допустим только как отдельная break-glass процедура
после проверки совместимости приложения и схемы.

## Диагностика календаря выплат

Проверьте последний результат синхронизации:

```bash
docker compose logs --tail=300 tracker | grep payout_calendar
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT event_type, currency, COUNT(*), MIN(payment_date), MAX(payment_date), MIN(fetched_at), MAX(fetched_at) FROM payout_calendar_events GROUP BY event_type, currency ORDER BY event_type, currency;"
```

Нормальное поведение:

- `payout_calendar_sync_completed` с `failed=0`;
- пустой календарь допустим, если по текущим позициям нет купонов или объявленных дивидендов;
- при ошибке отдельного инструмента старые строки сохраняются;
- `GetDividends` запрашивается с lookback по `record_date`, а итоговое окно фильтруется по `payment_date`.

5. Проверьте логи:

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

## Проверки данных после миграции

Проверьте основные runtime-таблицы:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d operations"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d income_events"
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
- ошибку выбора счёта: `TINKOFF_ACCOUNT_ID=auto` или пустое значение допустимы
  только при ровно одном открытом счёте; при нескольких укажите точный ID;
- отсутствие новых строк в `portfolio_snapshots`.

### `bot` не отправляет недельный или месячный отчёт

Проверьте:

```bash
docker compose logs --tail=200 bot
```

Что учитывать:

- ежедневный и утренний JobQueue используют timezone-aware время из `TIMEZONE`;
- в startup-логе проверьте `daily_job_schedule` и `yesterday_peak_alert_schedule`;
- системная таймзона контейнера не сдвигает эти два слота.

Для `tracker` дополнительно проверьте `TIMEZONE` и `SCHED_TZ`: локальная дата и
sync календаря выплат используют `TIMEZONE`, а cron-снапшот — `SCHED_TZ`. В
production задавайте им одинаковое значение.

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

### Отчёты расходятся с ручной суммой `operations`

Команды `/year`, `/month`, `/week`, `/today`, `/history`, `/twr` и trigger-ветки используют общий dedup-слой и только `OPERATION_STATE_EXECUTED`. Ручная сумма по сырой таблице может отличаться из-за отменённых/исполняющихся строк или дублей.

Для диагностики проверьте состояния и ключи:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT account_id, operation_id, COUNT(*) FROM operations GROUP BY account_id, operation_id HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT 20;"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT state, COUNT(*) FROM operations GROUP BY state ORDER BY state;"
```

## Проверки репозитория

Локальные проверки и CI-контракт описаны в
[CONTRIBUTING.md](../CONTRIBUTING.md). Перед развертыванием обязательно
проверьте итоговый Compose-конфиг с тем же env-файлом, который будет использован
при запуске:

```bash
export APP_ENV_FILE=.env
docker compose --env-file "$APP_ENV_FILE" config --quiet
```
