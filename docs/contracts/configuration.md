# Конфигурация runtime

- **Contract ID / версия:** `runtime_configuration.v1`
- **Статус:** active
- **Владельцы:** `config` (DB/T-Invest), service runtime (`tracker`, `bot`,
  `reporter`, `xray-client`), Compose (wiring)
- **Producer:** deployment environment / `.env`
- **Consumers:** Compose и процессы FinanceTracker
- **Проверка:** `.env.example`, `compose*.yml`, runtime parsers и config tests

## Источники и precedence

Production-конфигурация поступает из `${APP_ENV_FILE:-.env}`. Compose
`environment` поверх `env_file` задаёт внутренние service endpoints и mapping
`POSTGRES_* → DB_*`. Для БД непустой `DB_DSN` имеет приоритет над
`DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD`; DSN собирается через
`sqlalchemy.URL.create`, поэтому reserved characters credentials экранируются.

Значение, зафиксированное Compose в `environment`, является эффективным для
контейнера, даже если в `.env` существует другое значение. Config читается при
импорте/startup; hot reload не поддерживается.

## Secrets

| Переменная | Требование | Consumers |
|---|---|---|
| `POSTGRES_PASSWORD` / `DB_PASSWORD` / password в `DB_DSN` | обязательный непустой secret | db, migrate, tracker, bot, reporter |
| `TELEGRAM_BOT_TOKEN` | обязательный вне CI smoke | bot |
| `TINVEST_API_TOKEN` | обязательный вне container smoke | tracker |
| `REPORTER_SERVICE_KEY` | обязательный, минимум 16 символов; рекомендуются 32 random bytes | bot, reporter |
| `BOT_VLESS_URL`, `BOT_VLESS_FALLBACK_URL` | sensitive share links; обязательны только для включённых маршрутов | xray-client |

Secrets MUST NOT попадать в Git, логи, health payload, debug artifacts или
документацию. `.env.example` хранит только placeholders. Ротация reporter key
требует согласованного restart bot и reporter; несовпадение даёт `403`.

## Обязательные operator inputs

| Переменная | Формат / инвариант | Failure semantics |
|---|---|---|
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | непустые Compose inputs | Compose/DB либо application startup fail |
| `TELEGRAM_BOT_TOKEN` | Telegram token | bot fail-fast |
| `ALLOWED_USER_IDS` | comma-separated positive ASCII decimal IDs, без пустых элементов | bot не стартует с permissive default |
| `TINVEST_API_TOKEN` | API token | tracker fail-fast |
| `REPORTER_SERVICE_KEY` | secret ≥16 characters | Compose substitution/server/bot fail-fast |
| `FINANCETRACKER_BACKUP_DIR` | GitHub environment path вне checkout/volume | deploy прекращается до migrations |

## Database и execution

| Переменная | Default | Правило |
|---|---:|---|
| `DB_HOST` / `DB_PORT` | `db` / `5432` | внутренний endpoint; port integer |
| `DB_NAME` / `DB_USER` | `fintracker` / `aqua4` | используются при отсутствии `DB_DSN` |
| `DB_DSN` | empty | полный DSN имеет приоритет; PostgreSQL DSN обязан содержать host/user/database/password |
| `MIGRATIONS_DIR` | `/app/migrations` | существующий каталог forward migrations |
| `TIMEZONE` | `Europe/Moscow` | IANA zone; reporting, bot periods/jobs, tracker local dates, migration timezone |
| `SCHED_TZ` | `Europe/Moscow` | legacy fallback и timezone scheduler tracker; `TIMEZONE` имеет приоритет для `LOCAL_TZ`, но scheduler base всё ещё читает `SCHED_TZ` |
| `APP_ENV` | `dev` | environment label; `test` требуется для TLS break-glass |
| `APP_SERVICE` | service-specific | structured-log service label |
| `LOG_LEVEL` | `INFO` | Python log level |

Production SHOULD задавать одинаковые `TIMEZONE` и `SCHED_TZ`, пока legacy
`SCHED_TZ` не удалён: snapshot scheduler создаётся с `SCHED_TZ`, а локальные
reporting bounds — с `TIMEZONE`. Migration runner проверяет IANA timezone и
fail-fast; тихий UTC fallback tracker не является production guarantee.

## T-Invest и tracker

| Переменная | Default | Валидация / семантика |
|---|---:|---|
| `TINVEST_BASE_URL` | official REST URL | base URL read-only API |
| `TINVEST_ACCOUNT_STATUS` | `ACCOUNT_STATUS_ALL` | передаётся GetAccounts |
| `TINVEST_PORTFOLIO_CURRENCY` | `RUB` | requested portfolio currency |
| `TINKOFF_ACCOUNT_ID` | empty/`auto` | explicit ID MUST exist; auto допускает ровно один открытый account, иначе tracker fail. Reporter/bot при auto используют account последнего snapshot |
| `OPERATIONS_MAX_PAGES` | `10000` | min 1; достижение лимита при наличии next page откатывает transaction |
| `TINVEST_HTTP_TIMEOUT_SECONDS` | `20` | positive operational value expected |
| `TINVEST_HTTP_RETRY_TOTAL` | `3` | negative normalizes to 0 |
| `TINVEST_HTTP_BACKOFF_SECONDS` | `1` | negative normalizes to 0 |
| `TINVEST_HTTP_MAX_BACKOFF_SECONDS` | `60` | negative normalizes to 0 |
| `TINVEST_HTTP_POOL_CONNECTIONS` | `8` | min 1 |
| `TINVEST_HTTP_POOL_MAXSIZE` | `8` | min 1 |
| `TINVEST_INSTRUMENT_CACHE_TTL_SECONDS` | `86400` | min 0; 0 disables useful retention |
| `TINVEST_INSTRUMENT_CACHE_MAX_ENTRIES` | `1024` | min 0; 0 disables cache |
| `VERIFY_SSL` | `true` | strict boolean; `false` допустим только при `APP_ENV=test` и `ALLOW_INSECURE_TLS_FOR_TESTS=true` |
| `SNAPSHOT_MODE` | `interval` | `cron` selects daily schedule; other values use interval mode |
| `SNAPSHOT_INTERVAL_MINUTES` | `5` | integer; healthcheck normalizes minimum to 1 |
| `SNAPSHOT_HOUR` / `SNAPSHOT_MINUTE` | `23` / `30` | используются только в cron mode |
| `PAYOUT_CALENDAR_HORIZON_DAYS` | `90` | min 1 |
| `PAYOUT_DIVIDEND_RECORD_LOOKBACK_DAYS` | `365` | min 1 |
| `PAYOUT_CALENDAR_SYNC_HOUR` / `_MINUTE` | `9` / `0` | daily tracker schedule |

`TINKOFF_ACCOUNT_ID=auto` **не** означает «взять первый из нескольких».
Различие tracker (ровно один open account) и reporting (последний snapshot)
обеспечивает устойчивое чтение, но operator SHOULD закрепить ID при нескольких
счетах.

## Bot и расписания

| Переменная | Default | Валидация / семантика |
|---|---:|---|
| `DAILY_SUMMARY_HOUR` / `_MINUTE` | `18` / `0` | `datetime.time` range, timezone `TIMEZONE` |
| `YESTERDAY_PEAK_ALERT_HOUR` / `_MINUTE` | `8` / `0` | `datetime.time` range |
| `PAYOUT_WEEKLY_TIMEZONE` | `Europe/Moscow` | IANA zone |
| `PAYOUT_WEEKLY_HOUR` / `_MINUTE` | `10` / `0` | Monday digest |
| `PAYOUT_CALENDAR_TAX_RATE_PCT` | `13` | Decimal in `0..100` |
| `JOBQUEUE_SMOKE_TEST_ON_START` | `false` | truthy: `1,true,yes,on` |
| `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS` | `20` | integer seconds |
| `BOT_COMMAND_MAX_CONCURRENCY` | `2` | clamped to `1..8` |
| `BOT_COMMAND_TIMEOUT_SECONDS` | `120` | min 1 second |
| `BOT_STARTUP_RETRY_DELAY_SECONDS` | `15` | supervised restart delay |
| `BOT_READY_MAX_AGE_SECONDS` | `120` | readiness freshness |
| `BOT_READY_HEARTBEAT_SECONDS` | `30` | ready-file heartbeat |
| `ACCOUNT_FRIENDLY_NAME` | `Семейный капитал` | presentation only |
| `PLAN_ANNUAL_CONTRIB_RUB` | `400000` | numeric annual target |

## Reporter и Ollama

| Переменная | Default | Валидация / семантика |
|---|---:|---|
| `REPORTER_HOST` | `0.0.0.0` | Compose-internal listener; не публиковать host port |
| `REPORTER_PORT` | `8088` | integer, совпадает с `REPORTER_INTERNAL_URL` |
| `REPORTER_INTERNAL_URL` | `http://reporter:8088` | bot-only internal base URL |
| `REPORTER_MAX_BODY_BYTES` | `65536` | request body upper bound |
| `REPORTER_MAX_CONCURRENT_REQUESTS` | `2` | integer `1..32`, иначе startup fail |
| `REPORTER_SOCKET_TIMEOUT_SECONDS` | `10` | positive |
| `REPORTER_REQUEST_TIMEOUT_SECONDS` | `180` | positive; используется server build deadline и bot call timeout |
| `REPORT_PDF_ENGINE` | `weasyprint` | advertised/rendering backend |
| `OLLAMA_ENABLED` | `false` | narrative optional; deterministic fallback remains available |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | reporter endpoint |
| `OLLAMA_MODEL` | `qwen2.5:1.5b` | model identifier |
| `OLLAMA_TIMEOUT_SECONDS` | `60` | request timeout |
| `OLLAMA_KEEP_ALIVE` | `10m` | passed to Ollama |
| `OLLAMA_NUM_CTX` | `8192` | integer context request |
| `OLLAMA_MAX_INPUT_CHARS` | `12000` | bounded input/trimming threshold |

Debug artifacts (`REPORT_DEBUG_SAVE_HTML`, `REPORT_DEBUG_SAVE_PAYLOAD`) по
умолчанию выключены. При включении `REPORT_DEBUG_DIR` обязателен: это absolute
non-root path без traversal и symlink-компонентов, создаваемый с mode `0700`;
файлы имеют mode `0600` и очищаются по
`REPORT_DEBUG_MAX_FILES` (default 10) и `REPORT_DEBUG_MAX_AGE_SECONDS` (86400).
Artifacts содержат чувствительные финансовые данные.

## Proxy

`BOT_PROXY_ENABLED=false` оставляет bot без VLESS route. При включении
`xray-client` получает primary/fallback share links, слушает внутренний
`XRAY_LOCAL_PROXY_PORT=1080`; bot получает `BOT_PROXY_ENDPOINT` и no-proxy для
`db`, `tracker`, `reporter`, `xray-client`, localhost. Reporter client
дополнительно создаёт explicit empty `ProxyHandler`, поэтому internal PDF call
не должен уйти через Telegram proxy.

## Совместимость и failure semantics

Добавить необязательную переменную с безопасным default можно в `v1`. Изменить
default, единицу, precedence, secret classification, допустимый range или
трактовку существующего значения — contract change, требующий документации,
тестов и rollout note. Переменные test/smoke (`*_STARTUP_MODE`,
`ALLOW_INSECURE_TLS_FOR_TESTS`) MUST NOT использоваться как production bypass.

Парсинг числа/ZoneInfo/strict boolean, отсутствие secret или unsafe debug path
должны приводить к startup failure, а не к частично работающему сервису. Известное
исключение tracker UTC fallback перекрывается production migration validation и
не должно использоваться как recovery strategy.

## Проверка

```bash
python scripts/verify_compose_env.py
docker compose config
python -m unittest tests.test_compose_env_contract tests.test_tracker_config \
  tests.test_database_config tests.test_security_runtime_config \
  tests.test_bot_schedule_config tests.test_runtime_hardening_contracts
```
