# Карта проекта FinanceTracker

Карта отвечает на два вопроса: где находится источник истины для изменения и
какой модуль владеет затрагиваемой ответственностью. Runtime topology описана в
[ARCHITECTURE.md](ARCHITECTURE.md), опубликованные интерфейсы — в
[docs/contracts/](contracts/README.md).

## Корень репозитория

| Путь | Назначение | Источник истины для |
|---|---|---|
| `src/financetracker/` | Installable application package | Product и runtime implementation |
| `migrations/` | Forward-only PostgreSQL migrations | Фактический DDL и эволюция данных |
| `docker/` | Отдельные runtime images | OS/Python dependencies, user и image entrypoints |
| `requirements/` | Hash-pinned dependency sets по runtime/test profile | Воспроизводимая установка зависимостей |
| `tests/` | Unit, contract, integration и architecture tests | Автоматически проверяемое поведение и boundaries |
| `docs/` | Текущие contracts, architecture и operations | Человеко-читаемая спецификация |
| `scripts/` | Проверки и operator utilities | Predeploy backup, secret scan, Compose validation |
| `.github/workflows/` | CI, security scan и deployment pipeline | Delivery gates и rollout automation |
| `compose.yml` | Базовая deployment topology | Services, networks, volumes, healthchecks, hardening |
| `compose.ollama.yml` | Опциональный Ollama overlay | LLM network attachment reporter |
| `compose.ci.yml` | Ephemeral CI overlay | Container integration smoke environment |
| `pyproject.toml` | Package metadata и console scripts | Python version, package discovery, CLI entrypoints |
| `.env.example` | Безопасный шаблон конфигурации | Имена и примеры environment variables без secrets |

## Package layers

| Package | Владеет | Не владеет |
|---|---|---|
| `common` | Logging, UTC/time helpers, readiness files, runtime paths, нейтральные finance/text helpers | Product rules, SQL, external integrations |
| `domain` | Чистые расчёты assets, cashflows, operations, payouts, performance и rebalance | I/O, environment, Telegram, broker payloads |
| `config` | Typed parsing database и tracker HTTP settings | Соединения и use cases |
| `database` | SQLAlchemy base/models, session factory, canonical operation SQL, schema manifest | Scheduling и product presentation |
| `tracker` | T-Invest ingestion, normalization, reconciliation, scheduling и tracker health | Telegram и report rendering |
| `bot` | Telegram UI, scheduling/delivery, bot-owned writes, charts и dataset export | Broker ingestion и PDF rendering implementation |
| `reporting` | Monthly report read model, payload, narrative, charts, HTML/PDF и internal HTTP server | Telegram SDK и product writes |
| `xray` | VLESS parsing, Xray process lifecycle, SOCKS health/failover | Database и product data |

`common` и `domain` — нижние независимые слои. Product packages могут
использовать их, `config` и `database`, но не другие product packages. Точный
запрет импортов проверяет `tests/test_architecture_boundaries.py`.

## `tracker`: ingestion owner

| Модуль | Ответственность |
|---|---|
| `cli.py` | Console adapter `financetracker-tracker` |
| `app.py` | Composition root, startup validation и APScheduler registration |
| `runtime.py` | Tracker runtime state и orchestration текущих sync use cases |
| `tinvest_client.py` | T-Invest RPC/HTTP calls, retry, pagination и instrument cache |
| `http_session.py`, `http_policy.py` | Transport factory и чистая retry/backoff policy |
| `payloads.py` | Безопасный разбор broker JSON, MoneyValue/Quotation и URL metadata |
| `snapshot_sync.py` | Portfolio snapshot и position persistence |
| `operations_sync.py`, `income_events.py` | Operations upsert и income reconciliation |
| `payout_calendar_sync.py` | Coupon/dividend calendar synchronization |
| `asset_aliases.py` | Стабильная asset identity и alias persistence |
| `container_smoke.py`, `tracker_healthcheck.py` | CI smoke mode и readiness validation |
| `migrate.py` | SQL migration runner, checksum ledger и schema drift gate |
| `repair_operations_description_encoding.py` | Явно запускаемая maintenance-команда для repair description encoding |

Точки расширения:

- новый T-Invest method добавляется в `tinvest_client.py`, а broker decoding —
  в `payloads.py`;
- новый ingestion use case получает отдельный `*_sync.py` и подключается в
  composition root;
- изменение persisted structure начинается с новой migration, затем
  синхронизируются schema manifest, models и repositories;
- новые retry правила остаются в `http_policy.py`, чтобы проверяться без сети.

## `bot`: Telegram и delivery owner

| Группа | Модули | Ответственность |
|---|---|---|
| Entrypoint/runtime | `cli.py`, `entrypoint.py`, `bot.py`, `runtime.py` | Startup, proxy environment, Telegram application, command/job registration, DB session |
| Handlers | `handlers.py`, `iis_tax_deduction.py` | Authorization-aware Telegram adapters и callback actions |
| Product services | `summary_service.py`, `performance_service.py`, `year_summary_service.py`, `alerts_service.py`, `payout_service.py`, `rebalance_service.py` | Presentation use cases и orchestration доменных расчётов |
| Read repositories | `portfolio_repository.py`, `operations_repository.py`, `financials_repository.py`, `payout_repository.py`, `dataset_repository.py`, `reporting_account.py` | Тематические SQL read models и account resolution |
| Write/delivery repositories | `rebalance_repository.py`, `notification_repository.py`, `scheduled_job_runs.py` | Target weights, notification state, leases и job ledger |
| Scheduling | `scheduled_jobs.py`, `notification_jobs.py`, `notification_delivery.py`, `polling_watchdog.py` | Плановые задачи, catch-up, tracked delivery и polling recovery |
| Presentation | `today_templates.py`, `week_templates.py`, `month_templates.py`, `payout_calendar.py`, `charts.py` | Text/chart rendering без владения persistence |
| Exports/integration | `dataset.py`, `report_client.py`, `proxy_smoke.py` | Dataset archive, reporter HTTP client и Telegram route probe |
| Compatibility surfaces | `queries.py`, `services.py`, `jobs.py` | Стабильные import surfaces; новая логика размещается у тематического owner |

Точки расширения:

- новая команда: handler/use case → `COMMAND_SPECS` в `bot.py` → tests;
- новый read model: тематический repository, затем product service;
- новый scheduled notification: `scheduled_jobs`/`notification_jobs` с
  delivery key, lease/finalization semantics и startup catch-up;
- новый bot-owned write: отдельный repository и явная transaction boundary;
- новый внешний внутренний сервис: специализированный client module; transport
  details не размещаются в handlers.

## `reporting`: report owner

| Группа | Модули | Ответственность |
|---|---|---|
| Entrypoint/server | `cli.py`, `report_entrypoint.py`, `report_server.py` | Process startup, health endpoint, authenticated monthly PDF endpoint и request budgets |
| Runtime/read model | `runtime.py`, `repository.py` | Lazy DB sessions, timezone formatting primitives и read-only report queries |
| Payload | `report_payload.py`, `payload_identity.py`, `payload_timeseries.py`, `payload_activity.py`, `payload_summary.py`, `payload_ai_input.py` | Сборка versioned monthly payload по тематическим sections |
| Narrative | `narrative.py`, `report_ai.py` | Deterministic narrative и опциональный Ollama adapter/fallback |
| Rendering | `report_charts.py`, `chart_style.py`, `report_html.py`, `report_html_parts.py`, `report_pdf.py`, `report_artifact.py` | Charts → HTML → PDF → artifact |
| Orchestration | `report_pipeline.py` | Валидация периода и end-to-end report use case |
| Public adapter | `report_render.py` | Стабильная programmatic surface над rendering modules |
| Diagnostics | `debug_artifacts.py`, `formatting.py` | Bounded debug output и display formatting |

Точки расширения:

- новое поле monthly payload добавляется тематическому `payload_*` builder,
  агрегируется в `report_payload.py` и версионируется по compatibility policy;
- новый источник narrative реализуется adapter-ом с детерминированным fallback;
- новый render section проходит payload → charts/HTML parts → artifact tests;
- новый HTTP use case получает отдельный pipeline method, endpoint contract и
  authentication/resource limits; database writes reporter не добавляет.

## `database`: persistence owner

| Модуль/путь | Ответственность |
|---|---|
| `database/base.py` | Единый SQLAlchemy declarative base |
| `database/models.py` | Application ORM mappings и relationships |
| `database/session.py` | Engine/session factory |
| `database/operations.py` | Канонический deduplicated operations SQL |
| `database/schema_manifest.py` | Независимый expected catalog для post-migration drift check |
| `migrations/*.sql` | Последовательный production DDL; `*.rollback.sql` — только явно документированный recovery artifact |

Таблицы группируются по владельцу данных:

- portfolio: `instruments`, `portfolio_snapshots`, `portfolio_positions`;
- activity: `operations`, `income_events`, compatibility view `deposits`;
- identity/calendar: `asset_aliases`, `payout_calendar_events`;
- allocation: `rebalance_targets`, `invest_notifications`;
- delivery: `bot_daily_job_runs`, `bot_notification_deliveries`;
- schema control: `schema_migrations`.

Колонки, ключи, индексы, timezone и migration compatibility определены в
[контрактах данных](contracts/README.md), migrations и schema manifest.

## Entrypoints и lifecycle

| Console script | Python target | Lifecycle |
|---|---|---|
| `financetracker-migrate` | `tracker.migrate:main` | Одноразово перед application services |
| `financetracker-tracker` | `tracker.cli:main` | Долгоживущий scheduler process |
| `financetracker-bot` | `bot.cli:main` | Supervisor + Telegram child process |
| `financetracker-reporter` | `reporting.cli:main` | Долгоживущий bounded HTTP server |
| `financetracker-xray` | `xray.cli:main` | Долгоживущий proxy supervisor или disabled-mode process |
| `*-healthcheck` | Package-specific healthcheck | Короткий probe, запускаемый Compose |

Startup dependency chain:

```text
db healthy ─► migrate success ─┬─► tracker
                               ├─► reporter
                               └─► bot
xray-client healthy ──────────────► bot
```

В base Compose bot не ждёт health reporter на уровне `depends_on`; рабочий
запрос проверяет доступность reporter и возвращает контролируемую ошибку. CI
overlay добавляет reporter health dependency, потому что container smoke явно
проверяет bot → reporter boundary.

## Карта тестов к рискам

| Риск | Основные проверки |
|---|---|
| Нарушение package boundaries | `test_architecture_boundaries.py`, `test_reporter_dependency_boundary.py` |
| Drift schema/migrations | `test_tracker_migrations.py`, `test_migration_byte_identity.py`, `test_postgresql_integration.py` |
| Broken Compose/security | `test_compose_*`, `test_container_security_contract.py`, `test_runtime_hardening_contracts.py` |
| Несовместимый report payload/API | `test_report_payload.py`, `test_report_server.py`, `test_report_client.py`, contract schemas |
| Повторная/потерянная доставка | `test_notification_delivery_contracts.py`, `test_bot_daily_job_catchup.py` |
| Ошибка broker resilience | `test_tracker_api_resilience.py`, `test_tracker_http_policy.py`, `test_tracker_payloads.py` |
| Утечка secrets/PII в logs | `test_secret_scanning.py`, `test_logging_privacy_contract.py`, `test_runtime_logging_guardrails.py` |

## Как найти место изменения

1. Найдите внешний или data contract в [docs/contracts/](contracts/README.md).
2. По таблицам выше выберите owner package; не добавляйте peer import между
   `tracker`, `bot` и `reporting`.
3. Для persisted change начните с migration и schema contract; для runtime
   wiring — с `pyproject.toml`, Dockerfile и Compose.
4. Добавьте узкую проверку рядом с соответствующим risk suite и только затем
   расширяйте integration coverage.
5. Обновите эту карту, если изменились ownership, entrypoint, extension point
   или lifecycle; обновите [ARCHITECTURE.md](ARCHITECTURE.md), если изменился
   процесс, сеть, durable data flow или deployment boundary.
