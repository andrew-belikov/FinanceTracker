# Конфигурация FinanceTracker

Проект использует один файл `.env` в корне репозитория. Его читают:

- `docker compose` для подстановки значений в [`compose.yml`](../compose.yml);
- `tracker` и `bot` через `env_file: .env`;
- код приложений напрямую через `os.getenv(...)`.

## Как Разрешаются Значения

При стандартном запуске через `docker compose up -d --build` действует такой порядок:

1. `docker compose` читает `.env`.
2. Секция `env_file: .env` передаёт эти значения в контейнеры.
3. Секция `environment:` в `compose.yml` перекрывает одноимённые переменные из `env_file`.
4. Внутри `tracker` и `bot` переменная `DB_DSN`, если задана, перекрывает сборку DSN из `DB_*`.

Практические последствия:

- `POSTGRES_*` обязательны для контейнера `db`.
- В Docker-режиме `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` всё равно переопределяются из `compose.yml`.
- Если нужно изменить подключение приложений к БД без правки `compose.yml`, используйте `DB_DSN`.
- Эффективная переменная `APP_SERVICE` в контейнерах задаётся через `APP_SERVICE_TRACKER` и `APP_SERVICE_BOT`.

## Минимум Для Первого Старта

Для базового запуска через Docker заполните:

- `POSTGRES_PASSWORD`
- `TELEGRAM_BOT_TOKEN`
- `TINVEST_API_TOKEN`
- `ALLOWED_USER_IDS`

Остальные значения можно оставить по умолчанию и уточнить позже.

## Время И Планировщики

В проекте есть три разных времени:

- `TIMEZONE` используется ботом для форматирования дат и для части локальных периодов `/today`, `/week`, `/month`, `/year`, `/twr`.
- `SCHED_TZ` используется `tracker` для APScheduler и для вычисления `portfolio_snapshots.snapshot_date`.
- `bot` планирует ежедневный JobQueue в `18:00` по локальному времени контейнера через `datetime.now().astimezone().tzinfo`.

`TIMEZONE` не меняет время срабатывания ежедневного JobQueue. Если нужен, например, строго московский запуск авторассылок, настраивайте локальную таймзону контейнера отдельно. В текущем `compose.yml` отдельной переменной для этого не предусмотрено.

## Переменные Окружения

### Postgres И Compose

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `POSTGRES_DB` | обязательно для Docker | нет | `compose.yml` | Имя базы данных для контейнера `db`. В Docker-режиме также прокидывается в приложения как `DB_NAME`. |
| `POSTGRES_USER` | обязательно для Docker | нет | `compose.yml` | Пользователь Postgres для контейнера `db`. В Docker-режиме также прокидывается в приложения как `DB_USER`. |
| `POSTGRES_PASSWORD` | обязательно для Docker | нет | `compose.yml` | Пароль Postgres. В Docker-режиме также прокидывается в приложения как `DB_PASSWORD`. |

### Подключение Приложений К БД

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `DB_HOST` | опционально | `db` | `tracker`, `bot` | Хост Postgres. При штатном запуске через Docker Compose переопределяется значением `db`. |
| `DB_PORT` | опционально | `5432` | `tracker`, `bot` | Порт Postgres. При штатном запуске через Docker Compose переопределяется значением `5432`. |
| `DB_NAME` | опционально | `fintracker` | `tracker`, `bot` | Имя базы для приложений. При штатном запуске через Docker Compose переопределяется `POSTGRES_DB`. |
| `DB_USER` | опционально | `aqua4` | `tracker`, `bot` | Пользователь БД для приложений. При штатном запуске через Docker Compose переопределяется `POSTGRES_USER`. |
| `DB_PASSWORD` | опционально | `Q1a2z334` в коде | `tracker`, `bot` | Пароль БД для приложений. При штатном запуске через Docker Compose переопределяется `POSTGRES_PASSWORD`. |
| `DB_DSN` | опционально | собирается из `DB_*` | `tracker`, `bot` | Полный SQLAlchemy DSN. Если задан, код игнорирует `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. Это основной способ переопределить БД без правки `compose.yml`. |

Если пароль содержит спецсимволы, для `DB_DSN` используйте URL-encoding.

### Telegram И Доступ

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `TELEGRAM_BOT_TOKEN` | обязательно для работы бота | пусто | `bot` | Токен от `@BotFather`. При пустом значении бот падает при старте. |
| `ALLOWED_USER_IDS` | обязательно на практике | `365469` | `bot` | Список Telegram user_id через запятую. Используется и как белый список доступа, и как список чатов для авторассылок. |

### Время И Расписания

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `TIMEZONE` | опционально | `Europe/Moscow` | `bot` | Локальная таймзона для текстов и части date-based расчётов. Не влияет на время `run_daily(18:00)`. |
| `SCHED_TZ` | опционально | `Europe/Moscow` | `tracker` | Таймзона APScheduler у `tracker` и источник локальной даты для `snapshot_date`. Если `ZoneInfo` недоступен, код деградирует в `UTC`. |
| `SNAPSHOT_MODE` | опционально | `interval` | `tracker` | Режим планировщика: `interval` или `cron`. |
| `SNAPSHOT_INTERVAL_MINUTES` | обязательно для `interval`-режима | `5` | `tracker` | Интервал запуска `job_with_retry()` в минутах. |
| `SNAPSHOT_HOUR` | обязательно для `cron`-режима | `23` | `tracker` | Час запуска `CronTrigger`. Игнорируется в `interval`-режиме. |
| `SNAPSHOT_MINUTE` | обязательно для `cron`-режима | `30` | `tracker` | Минута запуска `CronTrigger`. Игнорируется в `interval`-режиме. |
| `JOBQUEUE_SMOKE_TEST_ON_START` | опционально | `false` | `bot` | Если `true`, бот один раз планирует тестовую отправку после старта. |
| `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS` | опционально | `20` | `bot` | Задержка перед smoke-test JobQueue. Используется только если включён `JOBQUEUE_SMOKE_TEST_ON_START`. |

### Портфель И Отчёты

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `ACCOUNT_FRIENDLY_NAME` | опционально | `Семейный капитал` | `bot` | Человекочитаемое имя счёта в текстах и заголовках графиков. |
| `PLAN_ANNUAL_CONTRIB_RUB` | опционально | `400000` | `bot` | Годовой план пополнений в рублях. Используется в `/week`, `/month`, `/year` и в триггере выполнения плана. |

### Invest API

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `TINVEST_API_TOKEN` | обязательно для `tracker` | пусто | `tracker` | Bearer token для Invest API. При пустом значении `tracker` падает при импорте модуля. |
| `TINVEST_BASE_URL` | опционально | `https://invest-public-api.tbank.ru/rest` | `tracker` | Базовый URL REST-шлюза T-Invest. |
| `TINVEST_PORTFOLIO_CURRENCY` | опционально | `RUB` | `tracker` | Валюта портфеля в запросе `GetPortfolio` и в `portfolio_snapshots.currency`. |
| `TINVEST_ACCOUNT_STATUS` | опционально | `ACCOUNT_STATUS_ALL` | `tracker` | Статус-фильтр в `UsersService/GetAccounts`. |
| `TINKOFF_ACCOUNT_ID` | опционально | пусто | `tracker` | Если задан и совпал с одним из `accounts[].id`, будет выбран именно этот счёт. При пустом или несовпавшем значении код возьмёт первый `ACCOUNT_STATUS_OPEN`, иначе просто первый счёт. Значение `auto` в текущем коде равносильно "не требовать точного совпадения". |
| `OPERATIONS_MAX_PAGES` | опционально | `10000` | `tracker` | Предохранитель от зависания при пагинации `GetOperationsByCursor`. |

### HTTP И Безопасность Подключения

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `VERIFY_SSL` | опционально | `false` | `tracker` | Включает или отключает SSL-проверку у `requests.post(...)`. При `false` код глушит `InsecureRequestWarning` и пишет предупреждение в лог. Используйте только в доверенной сети. |

### Логирование И Сервисная Идентичность

| Переменная | Обязательность | Default | Кто читает | Назначение и замечания |
| --- | --- | --- | --- | --- |
| `APP_ENV` | опционально | `dev` | `compose.yml`, `common/logging_setup.py` | Значение поля `env` в JSON-логах. По умолчанию compose передаёт `dev`. |
| `APP_SERVICE_TRACKER` | опционально | `iis_tracker` | `compose.yml` | Человекочитаемое имя сервиса для `tracker`. Через compose становится значением `APP_SERVICE`. |
| `APP_SERVICE_BOT` | опционально | `iis_tracker_bot` | `compose.yml` | Человекочитаемое имя сервиса для `bot`. Через compose становится значением `APP_SERVICE`. |
| `LOG_LEVEL` | опционально | `INFO` | `common/logging_setup.py` | Уровень root logger. |

### Эффективные Runtime-Переменные

Эти переменные читает код, но в Docker-режиме они обычно не задаются вручную в `.env`:

| Переменная | Кто читает | Откуда берётся | Замечание |
| --- | --- | --- | --- |
| `APP_SERVICE` | `common/logging_setup.py` | из `compose.yml` | Для `tracker` и `bot` значения разные. В обычном Docker-запуске используйте `APP_SERVICE_TRACKER` и `APP_SERVICE_BOT`, а не одну общую переменную `APP_SERVICE`. |

## Конфигурация И Схема Данных

Некоторые параметры имеют смысл только вместе с определённой схемой БД:

- `income_events` используется для купонов, дивидендов и части налогов. На чистой БД ORM создаёт эту таблицу автоматически.
- compatibility-view `deposits` появляется только после миграции `migrations/20260221_operations_from_deposits.sql`; без неё на чистой БД остаётся обычная таблица `deposits`, которую новый код не использует.
- глобальная уникальность `operations.operation_id` появляется только после миграции `migrations/20260304_operations_operation_item_fields.sql`.

Подробности о миграциях и об отличиях clean install от upgrade смотрите в [docs/ARCHITECTURE.md](ARCHITECTURE.md) и [docs/RUNBOOK.md](RUNBOOK.md).
