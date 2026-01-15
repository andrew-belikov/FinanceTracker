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
