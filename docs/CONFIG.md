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

- `TIMEZONE` — таймзона (например, `Europe/Moscow`).
- `DAILY_SUMMARY_HOUR`, `DAILY_SUMMARY_MINUTE` — время ежедневных уведомлений.
- `JOBQUEUE_SMOKE_TEST_ON_START` — одноразовый тест отправки через JobQueue при старте бота (`true/false`).
- `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS` — задержка перед smoke-test после старта (секунды).

Недельные отчёты отправляются по пятницам, месячные — в последний день месяца (логика внутри бота).

## Параметры портфеля/плана

- `ACCOUNT_FRIENDLY_NAME` — отображаемое имя счёта.
- `PLAN_ANNUAL_CONTRIB_RUB` — целевой план пополнений за год.
- `DRAWDOWN_ALERT_PCT` — порог уведомления о просадке (в процентах).

## Invest API

- `TINVEST_BASE_URL` — базовый URL API.
- `TINVEST_PORTFOLIO_CURRENCY` — валюта портфеля (обычно `RUB`).
- `TINVEST_ACCOUNT_STATUS` — фильтр статуса счёта (`ACCOUNT_STATUS_ALL` и т.п.).
- `TINKOFF_ACCOUNT_ID` — фиксированный account_id (если пусто/`auto`, выбирается первый доступный).
- `OPERATIONS_MAX_PAGES` — лимит страниц для синка операций.

## Сеть

- `VERIFY_SSL` — проверка SSL сертификата при запросах к API (`true/false`).

## Снапшоты

- `SNAPSHOT_INTERVAL_MINUTES` — интервал сохранения снапшотов (в минутах).
- `SNAPSHOT_HOUR`, `SNAPSHOT_MINUTE` — совместимость со старыми настройками (может не использоваться).

## Health-check «живости» данных

- Бот в ежедневном джобе проверяет дату последнего снапшота (`portfolio_snapshots.snapshot_date`).
- Алерт отправляется, если отставание больше 1 дня (порог зафиксирован в коде бота).
- Для операций используется sanity-check по таблице `operations`.
- В проверках учитываются типы пополнений (`operation_type`) из списка:
  - `OPERATION_TYPE_INPUT` (значение по умолчанию);
  - при расширении логики — дополнительные incoming-типы, явно заданные в коде бота.
- `operations` заполняется tracker-сервисом из API операций (включая историю).
- Если по учитываемым `operation_type` в `operations` нет данных, бот отправляет информационное сообщение.
- Жёсткий age-порог для операций **не** применяется, так как пополнения могут быть редкими.

### Совместимость `deposits`

- View `deposits` сохраняется только для legacy/backward compatibility старых SQL-запросов.
- Новый код и проверки должны читать данные операций из `operations`.

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
