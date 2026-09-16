# Reporter HTTP API

- **Contract ID / версия:** `monthly_report_service.v1`
- **Статус:** active, internal-only
- **Владелец / producer:** `financetracker.reporting.report_server`
- **Consumer:** `financetracker.bot.report_client`
- **Transport:** HTTP/1.1 внутри Compose network `bot_reporter_internal`
- **Машиночитаемый контракт:** [reporter.openapi.json](reporter.openapi.json)

## Граница доверия

Reporter не публикует host port. Доступ к endpoint выдачи PDF разрешён только
по заголовку `X-Reporter-Service-Key`; ключ передаётся только `bot` и `reporter`,
должен иметь не менее 16 символов и сравнивается constant-time до чтения тела.
`GET /healthz` не требует ключа и не содержит финансовых данных.

Bot MUST обращаться к reporter без наследования outbound proxy. Response и
request body MUST NOT кешироваться; server всегда отправляет
`Cache-Control: no-store`.

## `GET /healthz`

Возвращает `200 application/json`. Обязательное ядро:

- `status="ok"`, `service="reporter"`;
- `schema_version="monthly_report_service.v1"`;
- `pdf_engine`, `timezone`;
- эффективные параметры Ollama и debug-флаги.

Health показывает готовность HTTP-процесса и конфигурацию pipeline, но не
доказывает доступность БД, наличие снапшотов, успешный PDF render или Ollama.

## `POST /reports/monthly/pdf`

Создаёт monthly PDF для заданного или текущего локального месяца.

Request:

- authentication header обязателен;
- пустое тело эквивалентно `{}`;
- непустое тело — JSON object; если `Content-Type` указан, он должен содержать
  `application/json`;
- `year`: integer `1900..2100`, optional;
- `month`: integer `1..12`, optional;
- неизвестные поля сейчас игнорируются и не должны влиять на результат;
- размер тела ограничен `REPORTER_MAX_BODY_BYTES`.

Успех: `200 application/pdf` с `Content-Disposition: attachment; filename=...`,
`X-Report-Period: YYYY-MM`, `Cache-Control: no-store`. Имя текущего producer:
`fintracker_monthly_YYYY-MM.pdf`.

## Ошибки

| HTTP | `error` | Семантика / retry |
|---:|---|---|
| 400 | `invalid_request` | неверный JSON, body, тип/range; исправить request |
| 400 | `report_unavailable` | данные/период не позволяют собрать отчёт; не blind-retry |
| 401 | `authentication_required` | заголовок отсутствует; connection закрывается |
| 403 | `authentication_failed` | ключ неверен; connection закрывается |
| 404 | `not_found` | endpoint отсутствует; ошибка consumer/config |
| 408 | `request_timeout` | body не прочитан за socket timeout; retry допустим |
| 503 | `reporter_overloaded` | исчерпан concurrent budget; retry с backoff |
| 503 | `report_timeout` | build превысил deadline; статус завершения worker неизвестен |
| 500 | `report_render_failed` | renderer завершился ошибкой; alert, ограниченный retry |
| 500 | `report_failed` | необработанная ошибка pipeline; alert, ограниченный retry |

JSON error содержит обязательные `status="error"` и `error`; `message` и `path`
зависят от класса ошибки. При исчерпании connection budget сервер MAY закрыть
TCP-соединение без HTTP response. Consumer обязан трактовать transport error как
недоступность reporter, а не как успешную генерацию.

## Concurrency и deadlines

`REPORTER_MAX_CONCURRENT_REQUESTS` допустим в диапазоне `1..32`. Одновременно
принимается до удвоенного числа соединений; build budget равен настроенному
лимиту. `REPORTER_SOCKET_TIMEOUT_SECONDS` ограничивает чтение, а
`REPORTER_REQUEST_TIMEOUT_SECONDS` — ожидание результата build.

## Совместимость

Добавление необязательного response field или нового error code совместимо в
`v1`. Изменение path, method, authentication, media type, обязательного поля или
семантики периода требует новой major-версии. См. [общую политику](versioning.md).

## Проверка

- Producer: `src/financetracker/reporting/report_server.py`,
  `report_pipeline.py`.
- Consumer: `src/financetracker/bot/report_client.py`.
- Contract tests: `tests/test_report_server.py`, `tests/test_report_client.py`,
  `tests/test_report_pipeline.py`.
- Network boundary: `compose.yml`, `tests/test_reporter_dependency_boundary.py`.
