# Текущая архитектура

Этот документ описывает фактически работающую переходную архитектуру.
Утверждённое целевое состояние находится в
[`TARGET_ARCHITECTURE.md`](TARGET_ARCHITECTURE.md), нормативные границы — в
[`CONTRACTS.md`](CONTRACTS.md).

## Сервисы

- `db` (Postgres) — system of record для снапшотов, операций и delivery ledger.
- `migrate` — одноразово применяет и проверяет versioned SQL migrations.
- `tracker` — периодически опрашивает T-Invest read-only API и пишет данные в БД.
- `bot` — Telegram‑адаптер, расписание и delivery.
- `reporter` — аутентифицированный внутренний HTTP сервис monthly PDF.
- `xray-client` — изолированный proxy sidecar только для Telegram traffic.

## Поток данных

1. `tracker` получает портфель, операции и выплаты, затем сохраняет их в Postgres.
2. `bot` читает данные для Telegram-команд и управляет delivery ledger.
3. Для `/monthpdf` bot вызывает reporter по internal HTTP с service key.
4. `reporter` строит versioned payload, narrative и PDF; Telegram SDK в него не импортируется.

## Исходный layout

Runtime-код находится в installable namespace `src/financetracker/`: `common`,
`domain`, `tracker`, `bot`, `reporting` и `xray`. `reporting.repository`
владеет read-only SQL queries monthly PDF, а `domain` — общими чистыми
расчётами активов, cashflow и ребалансировки. Поэтому весь package `reporting`
не импортирует `bot`. Общие SQLAlchemy metadata, модели и factory engine/session
находятся в `financetracker.database`; `tracker.app` импортирует нужные модели
как потребитель persistence layer. Reporter использует
собственный lazy DB composition и presentation primitives, поэтому его payload
и renderer не импортируют Telegram runtime.

Внутри переходного package уже выделены из крупных модулей чистые границы:
`database.models` как единственный владелец SQLAlchemy-моделей,
`domain.performance` для TWR/XIRR/run-rate, `tracker.payloads` для разбора
T-Invest JSON, `tracker.http_policy` для чистой политики `Retry-After` и
exponential backoff, `domain.payouts` для расчёта ожидаемых выплат после налога,
`tracker.http_session` для создания переиспользуемого transport session,
`tracker.asset_aliases` для persistence стабильных идентификаторов брокерских
инструментов,
`reporting.formatting` для display-форматирования,
`reporting.payout_calendar` для независимого отображения календаря выплат и
`reporting.narrative` для deterministic fallback. Write-side SQL delivery ledger
(`claim`, `complete`, `release`, `heartbeat` и notification state) выделен в
`bot.notification_repository`; `bot.queries` остаётся read-моделью портфеля.
Persistence целевых весов ребалансировки выделен в
`bot.rebalance_repository` и не смешивается с portfolio read-моделью.
Read-модель календаря выплат находится в `bot.payout_repository`, а её
Telegram-представление — в `reporting.payout_calendar`.
AST-проверка в CI запрещает
обратные зависимости `common/domain → product/database` и peer-зависимости
между `tracker`, `reporting` и `bot`.

Параметры PostgreSQL разбираются единообразно в
`financetracker.config.database`; этот модуль не создаёт SQLAlchemy engine и не
открывает сетевые соединения. HTTP-настройки tracker разбираются отдельно в
`financetracker.config.tracker`, поэтому T-Invest client не является источником
env-значений.

Начальная PostgreSQL schema создаётся versioned migration
`20260220_portfolio_baseline.sql`; migration runner не вызывает ORM
`Base.metadata.create_all()`.
Канонический `OPERATIONS_DEDUP_CTE` и typed SQL helper живут в
`financetracker.database.operations`; product-слои используют только static SQL
с bind-параметрами и не держат собственные копии CTE.
После проверки ledger `migrate --check` сравнивает PostgreSQL catalog с
декларативным `financetracker.database.schema_manifest`; поэтому наличие всех
имён migration не маскирует удалённую колонку, изменённый тип или индекс.

## Container security

`tracker`, `bot`, `reporter` и `xray-client` выполняются от непривилегированного
пользователя `financetracker`. В Compose их root filesystem доступна только для
чтения, Linux capabilities сброшены, `no-new-privileges` включён, а временные
runtime-файлы ограничены tmpfs `/tmp`. Сертификаты tracker встраиваются в образ
во время build, поэтому runtime не требует прав на изменение system trust store.

## Логирование

- `tracker`, `bot`, `reporter` и `xray-client` используют единый JSON logger из `financetracker.common.logging_setup`.
- Startup helpers (`bot/entrypoint.py`, `bot/proxy_smoke.py`), healthcheck `xray-client` и maintenance scripts тоже пишут structured JSON logs.
- Каждая first-party runtime-запись содержит как минимум `ts`, `level`, `service`, `env`, `logger`, `event`, `msg`; дополнительный контекст идёт в `ctx`.
- Подробная спецификация и ТЗ для тиражирования стандарта находятся в
  [`LOGGING_STANDARD.md`](LOGGING_STANDARD.md), а машиночитаемая схема — в
  [`logging.schema.json`](logging.schema.json).
- Для first-party кода целевой контракт такой: `event` задаётся явно, остаётся стабильным и использует `snake_case`.
- Записи без явного `event` получают `event="auto_log"` и `ctx.event_source`; это fallback для stdlib/library logging, а не основной путь для project-owned кода.
- Значение `ctx.event_source="library"` используется для сторонних библиотек, `ctx.event_source="auto"` — для auto-tagging first-party записи.
- Для `xray-client` stdout/stderr дочернего процесса `xray` перехватываются и переизлучаются как события `xray_process_output`, поэтому контейнерный log stream остаётся JSON-only.
- Для bridged child-process логов имя потока передаётся в `ctx.stream`.
- Исключение: `financetracker.xray.render_config` — intentional data output, он печатает конфиг в stdout и не считается логированием.

## Примечания

- Данные Postgres живут в Docker volume (по умолчанию внешний `financetracker_fintracker-db`).
