# Конфигурация (.env)

Все параметры задаются через файл `.env` в корне проекта.

## Обязательные

- `POSTGRES_DB` — имя базы данных.
- `POSTGRES_USER` — пользователь Postgres.
- `POSTGRES_PASSWORD` — пароль пользователя Postgres.
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `TINVEST_API_TOKEN` — токен Invest API.
- `ALLOWED_USER_IDS` — список Telegram user_id, которым разрешена работа с ботом (через запятую).

## Расписание

- `TIMEZONE` — таймзона для отображения дат в текстах (например, `Europe/Moscow`).

Рассылка JobQueue запускается в `18:00` по времени хоста (локальная таймзона контейнера/сервера).
- `JOBQUEUE_SMOKE_TEST_ON_START` — одноразовый тест отправки через JobQueue при старте бота (`true/false`).
- `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS` — задержка перед smoke-test после старта (секунды).

Недельные отчёты отправляются по пятницам в 18:00, месячные — в последний день месяца в 18:00 (время хоста).

## Параметры портфеля/плана

- `ACCOUNT_FRIENDLY_NAME` — отображаемое имя счёта.
- `PLAN_ANNUAL_CONTRIB_RUB` — целевой план пополнений за год.

## Invest API

- `TINVEST_BASE_URL` — базовый URL API.
- `TINVEST_PORTFOLIO_CURRENCY` — валюта портфеля (обычно `RUB`).
- `TINVEST_ACCOUNT_STATUS` — фильтр статуса счёта (`ACCOUNT_STATUS_ALL` и т.п.).
- `TINKOFF_ACCOUNT_ID` — фиксированный account_id (если пусто/`auto`, выбирается первый доступный).
- `OPERATIONS_MAX_PAGES` — лимит страниц для синка операций.

## Сеть

- `VERIFY_SSL` — проверка SSL сертификата при запросах к API (`true/false`).
- `BOT_PROXY_ENABLED` — включает outbound proxy только для контейнера `bot` (`true/false`).
- `BOT_VLESS_URL` — VLESS+Reality share link для `xray-client`. Рекомендуется хранить значение в кавычках, чтобы `#label` в конце ссылки не отрезался парсером `.env`.

### Proxy только для `bot`

- При `BOT_PROXY_ENABLED=false` контейнер `bot` запускается без `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` и работает по старой схеме.
- При `BOT_PROXY_ENABLED=true` рядом поднимается сервис `xray-client`, а `bot` направляет только внешний HTTP(S)-трафик через `http://xray-client:3128`.
- Внутренние адреса (`localhost`, `127.0.0.1`, `db`, `tracker`, `xray-client`) добавляются в `NO_PROXY`, поэтому внутренние обращения не уходят в proxy.
- `tracker` и `db` не получают proxy env и продолжают работать напрямую.

Быстрая проверка:

```bash
docker compose ps
docker compose logs --tail=100 xray-client
docker compose exec bot python proxy_smoke.py
```

Ожидаемо:
- при `BOT_PROXY_ENABLED=true` `xray-client` в `healthy`;
- при `BOT_PROXY_ENABLED=false` `xray-client` не виден в обычном `docker compose ps`, а `docker compose ps -a xray-client` показывает `Exited (0)`;
- все runtime-процессы проекта пишут JSON Lines в `stdout`, включая `xray-client`, startup smoke, healthcheck и maintenance scripts;
- в логах `xray-client` есть события `xray_proxy_ready`, `xray_telegram_smoke_completed` и `xray_process_output`;
- `proxy_smoke.py` подтверждает доступность Telegram API и прямой TCP-доступ к `db` через событие `bot_startup_smoke_completed` или `bot_startup_smoke_failed`.

## Снапшоты

- `SNAPSHOT_INTERVAL_MINUTES` — интервал сохранения снапшотов (в минутах).
- `SNAPSHOT_HOUR`, `SNAPSHOT_MINUTE` — совместимость со старыми настройками (может не использоваться).

### Совместимость `deposits`

- View `deposits` сохраняется только для legacy/backward compatibility старых SQL-запросов.
- Новый код и проверки должны читать данные операций из `operations`.

### Поля инструмента в `operations`

- Для операций поддерживаются поля `instrument_uid` и `figi`.
- Если поля добавлены миграцией `migrations/20260225_operations_add_instrument_columns.sql`, tracker
  при синхронизации обновляет не только новые операции, но и делает backfill существующих строк,
  где эти поля ещё пустые.


### Поля `OperationItem` в `operations`

- Миграция `migrations/20260304_operations_operation_item_fields.sql` добавляет поля из `OperationItem`
  (кроме массива `trades_info.trades`).
- Tracker использует `GetOperationsByCursor` с `withoutTrades=true`, постранично обходит `nextCursor`
  и делает upsert по `operation_id`.
- Если после миграции у исторических строк новые поля ещё пустые (`state IS NULL`), tracker
  автоматически делает backfill от даты открытия счёта, затем возвращается к инкрементальной синхронизации.

### Миграция на новую схему (кратко)

```bash
# 1) Остановить сервисы, пишущие/читающие БД
docker compose stop tracker bot

# 2) Применить миграцию
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f migrations/20260221_operations_from_deposits.sql

# 3) Запустить сервисы обратно
docker compose up -d tracker bot

# 4) Проверить, что операции читаются из operations
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
```

Ожидаемый результат: в `operations` есть записи по типам пополнений; `deposits` используется только как совместимый legacy-view.
