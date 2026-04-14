# Конфигурация (.env)

Все параметры задаются через файл `.env` в корне проекта.
Все команды ниже предполагают запуск `docker compose` из корня репозитория.

## Обязательные

- `POSTGRES_DB` — имя базы данных.
- `POSTGRES_USER` — пользователь Postgres.
- `POSTGRES_PASSWORD` — пароль пользователя Postgres.
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `TINVEST_API_TOKEN` — токен Invest API.
- `ALLOWED_USER_IDS` — список Telegram user_id, которым разрешена работа с ботом (через запятую).

## Расписание

- `TIMEZONE` — таймзона для отображения дат в текстах и для расписания JobQueue (например, `Europe/Moscow`).
- `DAILY_SUMMARY_HOUR` — час ежедневного запуска JobQueue в таймзоне `TIMEZONE` (по умолчанию `18`).
- `DAILY_SUMMARY_MINUTE` — минута ежедневного запуска JobQueue в таймзоне `TIMEZONE` (по умолчанию `0`).

Рассылка JobQueue запускается по таймзоне `TIMEZONE`; по умолчанию это `18:00`.
- `JOBQUEUE_SMOKE_TEST_ON_START` — одноразовый тест отправки через JobQueue при старте бота (`true/false`).
- `JOBQUEUE_SMOKE_TEST_DELAY_SECONDS` — задержка перед smoke-test после старта (секунды).

Недельные отчёты отправляются по пятницам, месячные — в последний день месяца. Время отправки задаётся через `DAILY_SUMMARY_HOUR` / `DAILY_SUMMARY_MINUTE` в таймзоне `TIMEZONE`.

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

- `VERIFY_SSL` — проверка SSL сертификата при запросах к API (`true/false`). Рекомендуемое значение: `true`.
- `BOT_PROXY_ENABLED` — включает outbound proxy только для контейнера `bot` (`true/false`).
- `BOT_VLESS_URL` — основной VLESS share link для `xray-client`. Рекомендуется хранить значение в кавычках, чтобы `#label` в конце ссылки не отрезался парсером `.env`.
- `BOT_VLESS_FALLBACK_URL` — дополнительный VLESS share link. Если основной `BOT_VLESS_URL` не проходит render/startup smoke или активный маршрут позже деградирует, `xray-client` автоматически пробует следующий кандидат.
- `BOT_STARTUP_RETRY_DELAY_SECONDS` — пауза между supervised-перезапусками процесса `bot.py`, если Telegram API временно недоступен через proxy или прямой транспорт (по умолчанию `15` секунд).

Для `tracker` при старте контейнера автоматически устанавливаются доверенные сертификаты из каталога `docker/certs/`, поэтому обычный deploy через `docker compose up -d --build --force-recreate --remove-orphans` пересоздаёт контейнер уже с актуальной trust store.

### Proxy только для `bot`

- При `BOT_PROXY_ENABLED=false` контейнер `bot` запускается без `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` и работает по старой схеме.
- При `BOT_PROXY_ENABLED=true` рядом поднимается сервис `xray-client`, а `bot` направляет Telegram-трафик через локальный SOCKS endpoint `socks5h://xray-client:1080`.
- `xray-client` принимает до двух кандидатных ссылок: основную `BOT_VLESS_URL` и fallback `BOT_VLESS_FALLBACK_URL`. Кандидаты проверяются по очереди, активной остаётся первая ссылка, которая успешно прошла startup smoke; дальше сервис продолжает runtime-проверки и при повторяющихся сбоях переключается на следующий кандидат.
- Основной и fallback URL могут использовать разные transport/security-настройки. Текущий парсер поддерживает Reality/TCP и VLESS с `security=none`, включая `type=kcp`.
- Внутренние адреса (`localhost`, `127.0.0.1`, `db`, `tracker`, `xray-client`) добавляются в `NO_PROXY`, поэтому внутренние обращения не уходят в proxy.
- Long polling (`getUpdates`) и обычные Bot API запросы используют один и тот же явный proxy endpoint из `BOT_PROXY_ENDPOINT`; это снижает риск зависшего polling при переезде между хостами.
- Если `bot.py` не может инициализироваться из-за транспортного `TimedOut` / `NetworkError`, `entrypoint.py` не завершает весь контейнер сразу, а перезапускает сам процесс бота с паузой `BOT_STARTUP_RETRY_DELAY_SECONDS`.
- Если watchdog два раза подряд видит backlog Telegram updates при превышении порога стагнации, `bot` завершает процесс и рассчитывает на автоматический рестарт контейнера через `restart: unless-stopped`.
- `xray-client` проверяет не только локальный порт, но и outbound-маршрут через `XRAY_HEALTHCHECK_URL`; в compose по умолчанию используется `https://api.ipify.org`.
- `tracker` и `db` не получают proxy env и продолжают работать напрямую.

## PDF-отчеты / Ollama

Отдельный сервис `reporter` собирает PDF-отчеты и AI-нарратив. Он читает БД напрямую и подключается к локальной Ollama по внешней Docker-сети `localllm_localllm`.
Изнутри контейнера `reporter` нужно использовать `OLLAMA_BASE_URL=http://ollama:11434`; `localhost` внутри контейнера не указывает на Ollama.

- `OLLAMA_ENABLED` — включает AI-слой в `reporter` (`true/false`). Если `false`, отчет должен собираться на детерминированном fallback-тексте.
- `OLLAMA_BASE_URL` — базовый URL Ollama. Для `homeserver` и внешней сети по умолчанию: `http://ollama:11434`.
- `OLLAMA_MODEL` — модель Ollama для narrative-слоя.
- `OLLAMA_TIMEOUT_SECONDS` — timeout HTTP-запроса к Ollama.
- `OLLAMA_KEEP_ALIVE` — как долго Ollama держит модель в памяти после запроса.
- `OLLAMA_NUM_CTX` — размер контекстного окна, если сервис передает его в запросе.
- `REPORT_PDF_ENGINE` — backend для сборки PDF (`weasyprint` в v1).
- `REPORT_TMP_DIR` — рабочая директория для промежуточных HTML/PDF-артефактов.
- `REPORT_DEBUG_SAVE_HTML` — сохранять промежуточный HTML рядом с PDF (`true/false`).
- `REPORT_DEBUG_SAVE_PAYLOAD` — сохранять JSON payload отчета (`true/false`).

Если `reporter` запускается в той же среде, где живет Ollama compose-проект, внешний network `localllm_localllm` должен быть создан заранее и доступен Docker'у. На `homeserver` он уже используется для доступа к `http://ollama:11434`.

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
- в логах `xray-client` есть события `xray_proxy_ready`, `xray_proxy_smoke_completed`, `xray_runtime_smoke_failed`, `xray_runtime_failover_scheduled` и `xray_process_output`;
- при наличии fallback-ссылки в логах и status file появляется `active_link_role` со значением `primary` или `fallback`;
- `proxy_smoke.py` подтверждает доступность Telegram API и прямой TCP-доступ к `db` через событие `bot_startup_smoke_completed` или `bot_startup_smoke_failed`.

## Structured logging

- `APP_SERVICE` определяет поле `service` в JSON-логах; для `xray-client` оно фиксируется как `xray_client` в compose-конфиге.
- `APP_ENV` определяет поле `env`; по умолчанию используется `dev`, если переменная не задана.
- First-party код должен писать явные события в формате `snake_case` через общий logger из `src/common/logging_setup.py`.
- Fallback `event="auto_log"` допустим только для записей без явного события, обычно от сторонних библиотек.
- В таких fallback-записях formatter добавляет `ctx.event_source`: `library` для сторонних библиотек и `auto` для auto-tagging first-party записи, если код не задал `event` явно.
- Для дочерних процессов строки stdout/stderr оборачиваются в JSON и получают `ctx.stream`.
- `src/xray_client/render_config.py` остаётся исключением: он печатает конфиг в stdout как полезный data output, а не как лог.

## Снапшоты

- `SNAPSHOT_INTERVAL_MINUTES` — интервал сохранения снапшотов (в минутах).
- `SNAPSHOT_HOUR`, `SNAPSHOT_MINUTE` — совместимость со старыми настройками (может не использоваться).

### Исторический compatibility-слой `deposits`

- View `deposits` относится только к исторической SQL-миграции со старой схемы.
- Активный runtime-код и текущие проверки должны читать данные операций из `operations`.

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

### Историческая миграция со схемы `deposits` (кратко)

```bash
# 1) Остановить сервисы, пишущие/читающие БД
docker compose stop tracker bot

# 2) Применить миграцию
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260221_operations_from_deposits.sql

# 3) Запустить сервисы обратно
docker compose up -d tracker bot

# 4) Проверить, что операции читаются из operations
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
```

Ожидаемый результат: в `operations` есть записи по типам пополнений; `deposits` при наличии остаётся только историческим compatibility-view.
