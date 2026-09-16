# Контракты внешних интеграций

Документ фиксирует границы FinanceTracker с внешними системами. Он не
переопределяет upstream API, а описывает только используемое подмножество,
локальные гарантии, отказоустойчивость и требования безопасности.

## Общие требования

- Секреты MUST передаваться только через runtime-конфигурацию и MUST NOT
  попадать в логи, fixtures или документацию.
- В логах разрешены только allowlisted metadata: имя метода, host/path, HTTP
  status, размеры и технические correlation identifiers. Тела upstream-ответов
  не логируются.
- Timeout, retry и fallback MUST быть ограничены конфигурацией. Бесконечные
  повторы запрещены.
- Ошибка внешней системы MUST NOT приводить к частичному подтверждённому
  результату, если полнота данных является частью операции.
- Изменение используемого метода, transport, формата запроса или локальной
  семантики ответа требует обновления этого документа и соответствующих тестов.

## T-Invest API

| Поле | Контракт |
|---|---|
| Producer | T-Invest Invest API |
| Consumer | `financetracker.tracker` |
| Transport | HTTPS POST, JSON, `Authorization: Bearer …` |
| Режим | только чтение; торговые методы и выставление заявок не используются |
| Конфигурация | `TINVEST_API_TOKEN`, `TINVEST_BASE_URL`, `TINVEST_HTTP_*`, `VERIFY_SSL` |

Используемые RPC:

- `UsersService/GetAccounts` — выбор доступного счёта;
- `OperationsService/GetPortfolio` — текущий портфель;
- `OperationsService/GetOperationsByCursor` — синхронизация операций;
- `InstrumentsService/GetInstrumentBy` — метаданные инструмента;
- `InstrumentsService/GetBondCoupons` — календарь купонов;
- `InstrumentsService/GetDividends` — календарь дивидендов.

Локальные гарантии:

- автоматические повторы разрешены только потому, что все текущие методы
  read-only;
- повторяются transport errors и HTTP `408`, `429`, `5xx`; число попыток и
  exponential backoff ограничены конфигурацией, `Retry-After` учитывается;
- HTTP status, отличный от `200`, невалидный JSON и неверная структура
  обязательных коллекций завершают операцию ошибкой;
- пагинация операций ограничена `OPERATIONS_MAX_PAGES`; повторяющийся или
  отсутствующий cursor при `hasNext=true` считается нарушением контракта;
- неполная синхронизация операций не коммитится как успешная;
- `TINKOFF_ACCOUNT_ID=auto` допустим только при ровно одном открытом счёте.

Добавление мутационного RPC требует отдельной политики идемпотентности: текущий
retry-контракт к нему неприменим.

## Telegram Bot API

| Поле | Контракт |
|---|---|
| Producer/consumer | Telegram Bot API и `financetracker.bot` |
| Transport | long polling и Bot API через `python-telegram-bot` |
| Аутентификация | `TELEGRAM_BOT_TOKEN` |
| Авторизация пользователя | allowlist `ALLOWED_USER_IDS` |
| Доставка | текстовые сообщения, документы и синхронизация списка команд |

Локальные гарантии:

- update от пользователя вне allowlist игнорируется без раскрытия данных;
- `ALLOWED_USER_IDS` одновременно задаёт получателей автоматических сообщений;
- polling и обычные Bot API requests используют раздельные connection pools и
  timeout-настройки;
- Markdown-ошибка при отправке текста приводит к одной повторной отправке того
  же текста без `parse_mode`;
- transport failure при старте или polling возвращает специальный exit code,
  чтобы рестарт выполнял supervisor;
- повтор доставки после неоднозначного transport failure не должен считаться
  доказательством exactly-once. Для фоновых уведомлений локальная БД хранит
  состояние claim/delivery/finalization.

Пользовательские команды, формулы и fallback-ответы являются отдельным
[контрактом поведения](../BEHAVIOR.md).

## Ollama API

| Поле | Контракт |
|---|---|
| Producer | Ollama-compatible HTTP API |
| Consumer | `financetracker.reporting` |
| Endpoint | `POST {OLLAMA_BASE_URL}/api/chat` |
| Статус | optional; включается только через `OLLAMA_ENABLED` |
| Формат | non-streaming JSON с JSON Schema в поле `format` |

Локальные гарантии:

- `OLLAMA_BASE_URL` должен быть абсолютным HTTP(S) URL без credentials;
- модель получает только подготовленный и ограниченный по размеру набор фактов;
- модель формирует narrative, но не является источником финансовых чисел;
- ответ нормализуется и проходит structural и semantic validation;
- после первой ошибки валидации допускается одна repair-попытка;
- disabled, transport error или повторная validation error приводят к
  детерминированному fallback без отказа PDF pipeline.

Внешний AI-текст не меняет канонический
[monthly report payload](monthly-report-payload.md).

## VLESS/Xray transport

| Поле | Контракт |
|---|---|
| Producer | primary/fallback VLESS endpoints |
| Consumer | `xray-client`, затем только `bot` через SOCKS |
| Внутренняя граница | Docker-only SOCKS listener, без host port publishing |
| Конфигурация | `BOT_VLESS_URL`, optional `BOT_VLESS_FALLBACK_URL`, `XRAY_*` |

Локальные гарантии:

- proxy включается только явно; без URL при включённом режиме startup завершается
  ошибкой;
- VLESS URL валидируется до запуска Xray; поддерживаемые security — `none` и
  `reality`, network — `tcp`, `raw`, `kcp`;
- секретная часть URL и user id не попадают в status/log summary;
- readiness требует поднятого локального SOCKS listener и успешного smoke через
  proxy;
- runtime smoke после заданного числа ошибок переключает primary/fallback;
- proxy network доступна `bot`, но не используется `tracker`, `reporter` и БД;
- stdout/stderr Xray переизлучаются через общий контракт структурированных логов.

## Источники истины и проверка

| Интеграция | Реализация | Основные проверки |
|---|---|---|
| T-Invest | `tracker/tinvest_client.py`, `tracker/operations_sync.py` | `test_tracker_api_resilience.py`, `test_payout_calendar.py` |
| Telegram | `bot/bot.py`, `bot/runtime.py`, `bot/jobs.py` | `test_bot_commands.py`, `test_bot_startup_resilience.py`, `test_notification_delivery_contracts.py` |
| Ollama | `reporting/report_ai.py` | `test_report_ai.py`, `test_report_pipeline.py` |
| VLESS/Xray | `xray/entrypoint.py`, `xray/render_config.py`, Compose topology | `test_xray_proxy_config.py`, `test_compose_healthchecks.py`, Compose config check |

Имена переменных, defaults и fail-fast правила находятся в
[контракте конфигурации](configuration.md). Формат operational events — в
[контракте логирования](../LOGGING_STANDARD.md).
