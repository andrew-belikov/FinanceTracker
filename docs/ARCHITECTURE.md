# Архитектура FinanceTracker

Этот документ фиксирует только текущую runtime-архитектуру. Карта каталогов и
владельцев модулей вынесена в [PROJECT_MAP.md](PROJECT_MAP.md), формальные
интерфейсы — в [каталог контрактов](contracts/README.md), эксплуатационные
процедуры — в [RUNBOOK.md](RUNBOOK.md).

## Архитектурный стиль

FinanceTracker — модульный Python-монолит, который поставляется как один
installable package `financetracker`, но запускается несколькими изолированными
процессами. Процессы не разделяют память: интеграция идёт через PostgreSQL и
один внутренний HTTP API. Docker Compose задаёт production topology и порядок
старта.

```text
T-Invest API
     │ HTTPS
     ▼
 tracker ───── writes ─────┐
                           ▼
 migrate ─────────────► PostgreSQL ◄──── reads/writes ──── bot ───► Telegram API
                           ▲                                  │        ▲
                           │ reads                            │ HTTP   │
                           │                                  ▼        │
                        reporter ◄────────────────────────────┘   xray-client
                           │                                      (optional proxy)
                           └──────── optional HTTP ───────► Ollama
```

Стрелка показывает инициатора соединения. PostgreSQL — единственный durable
system of record. PDF и временные файлы bot/reporter живут в container tmpfs и
не являются постоянным хранилищем.

## Runtime-компоненты

| Компонент | Entry point | Ответственность | Состояние и внешние связи |
|---|---|---|---|
| `db` | `postgres:16` | Хранение портфеля, операций, календаря выплат, настроек ребалансировки и delivery ledger | External Docker volume `financetracker_fintracker-db`; порт наружу не публикуется |
| `migrate` | `financetracker-migrate` | Последовательное применение forward-only SQL migrations под advisory lock; проверка checksum ledger и schema manifest | Одноразовый процесс; владеет schema changes, migration data transforms и `schema_migrations` |
| `tracker` | `financetracker-tracker` | Выбор брокерского счёта, загрузка портфеля/операций/выплат из T-Invest, нормализация и запись снапшотов | Исходящий HTTPS к T-Invest; читает и пишет PostgreSQL |
| `bot` | `financetracker-bot` | Telegram-команды, плановые отчёты и уведомления, dataset export, управление целями ребалансировки | Telegram long polling; читает product data, пишет bot-owned state; вызывает `reporter` |
| `reporter` | `financetracker-reporter` | Сбор monthly payload, narrative, графиков, HTML и PDF | Read-only product queries к PostgreSQL; внутренний HTTP; опциональный Ollama |
| `xray-client` | `financetracker-xray` | Локальный SOCKS5 endpoint для Telegram traffic, health probe и failover между VLESS routes | Не обращается к БД; единственный компонент в отдельной egress-сети proxy path |

Канонические console scripts объявлены в
[`pyproject.toml`](../pyproject.toml). Образы и их системные зависимости заданы
Dockerfiles (например,
[`Dockerfile.tracker`](../docker/Dockerfile.tracker)), а service wiring — в
[`compose.yml`](../compose.yml).

## Границы процессов и сети

- `db`, `migrate`, `tracker`, `bot` и `reporter` используют Compose default
  network для доступа к PostgreSQL. Database port не публикуется на host.
- `bot` и `reporter` дополнительно подключены к internal network
  `bot_reporter_internal`. Reporter публикует `8088` только внутри Compose.
- `bot` и `xray-client` подключены к internal network `bot_proxy_internal`.
  При включённом proxy bot направляет внешний Telegram traffic на
  `socks5h://xray-client:1080`; адреса внутренних сервисов входят в `NO_PROXY`.
- `xray-client` подключён к `xray_egress`. При выключенном proxy он сохраняет
  тот же runtime contract, но не запускает внешний tunnel.
- Опциональный [`compose.ollama.yml`](../compose.ollama.yml) подключает только
  `reporter` к внешней Ollama network. Базовый deployment от неё не зависит.
- [`compose.ci.yml`](../compose.ci.yml) заменяет persistent volume на
  ephemeral volume, закрывает egress-сети и включает container-smoke modes;
  production deployment этот override не использует.

Runtime-контейнеры `tracker`, `bot`, `reporter` и `xray-client` работают от
непривилегированного UID, с read-only root filesystem, без Linux capabilities,
с `no-new-privileges` и ограниченным `/tmp` в tmpfs. Readiness `tracker` и `bot`
передаётся через файлы в этом tmpfs; reporter и xray проверяются активными
health probes.

## Потоки данных

### Ingestion

1. `tracker` получает account, portfolio, operations, instruments, coupons и
   dividends через T-Invest HTTP client.
2. Payload helpers приводят broker values к доменным типам; asset aliases
   стабилизируют identity между FIGI, instrument UID и asset UID.
3. Use cases `snapshot_sync`, `operations_sync` и `payout_calendar_sync`
   выполняют idempotent upsert/reconciliation в PostgreSQL.
4. После успешного цикла tracker обновляет readiness marker. Планировщик
   повторяет snapshot/operations sync и отдельно calendar sync.

### Telegram read model и delivery

1. Telegram update поступает в `bot`, проходит allowlist и command routing.
2. Handler вызывает product service; service читает данные через тематический
   repository и использует чистые расчёты из `domain`.
3. Ответ отправляется в Telegram. Плановые job используют PostgreSQL leases и
   delivery records, чтобы отслеживать владение попыткой и итог доставки.
4. Команды изменения целевых весов и классификации налогового вычета в операции —
   контролируемые write paths bot; остальные presentation use cases читают
   product data.

### Monthly PDF

1. `bot.report_client` отправляет `POST /reports/monthly/pdf` с годом/месяцем и
   `X-Reporter-Service-Key` через internal network, явно минуя proxy.
2. `reporter` ограничивает размер body, число параллельных запросов и время
   выполнения, затем собирает versioned payload из read-only SQL queries.
3. Narrative строится детерминированно либо через опциональный Ollama с
   fallback; затем формируются charts, HTML и PDF.
4. Reporter возвращает PDF bytes; bot сохраняет их во временный файл, отправляет
   документ в Telegram и удаляет локальный artifact по завершении use case.

HTTP и payload shapes перечислены в
[каталоге контрактов](contracts/README.md). Пользовательское поведение отчёта
описано в [PDF_REPORT.md](PDF_REPORT.md).

## Данные и владение записью

SQL migrations — единственный production-механизм изменения схемы. ORM models
описывают application mapping, а независимый `database.schema_manifest`
проверяет обязательные таблицы, колонки, ключи, индексы и view после применения
migrations.

| Набор данных | Основной writer | Readers |
|---|---|---|
| `instruments`, `portfolio_snapshots`, `portfolio_positions`, `operations`, `asset_aliases`, `payout_calendar_events` | `tracker` | `bot`, `reporter` |
| `income_events` | `tracker` (reconciliation), `bot` (notification status) | `bot`, `reporter` |
| `rebalance_targets` | `bot` | `bot`, `reporter` |
| `invest_notifications` | `bot` | `bot` |
| `bot_daily_job_runs`, `bot_notification_deliveries` | `bot` | `bot` |
| `schema_migrations` и database objects | `migrate` | `migrate --check`, application runtimes |

Детальная схема, инварианты identity, timezone и compatibility находятся в
[контрактах данных](contracts/README.md). Источником DDL остаются versioned SQL
migrations (начиная с
[`20260220_portfolio_baseline.sql`](../migrations/20260220_portfolio_baseline.sql)),
а машинно проверяемого expected state —
[`database/schema_manifest.py`](../src/financetracker/database/schema_manifest.py).

## Зависимости модулей

Допустимое направление зависимостей:

```text
common      domain
   ▲           ▲
   └────┬──────┘
        │
config  database
   ▲       ▲
   └───┬───┘
       │
tracker   bot   reporting   xray
```

- `common` и `domain` не импортируют product packages или persistence layer.
- `tracker`, `bot` и `reporting` не импортируют друг друга. Межпроцессная связь
  bot → reporter реализована только HTTP-клиентом.
- `database` владеет SQLAlchemy metadata/models, session factory, canonical
  operations SQL и schema manifest.
- `config` разбирает конфигурацию без открытия сетевых соединений.
- `xray` зависит только от общих runtime utilities и не знает о product data.

Эти правила закреплены в
[`tests/test_architecture_boundaries.py`](../tests/test_architecture_boundaries.py).

## Отказоустойчивость и наблюдаемость

- Все first-party runtime-процессы пишут structured JSON logs по
  [LOGGING_STANDARD.md](LOGGING_STANDARD.md); чувствительные значения не входят
  в log context.
- Compose запускает application services только после healthy database и
  успешного `migrate`. Ошибка migration останавливает rollout до старта
  потребителей.
- Tracker использует bounded HTTP retries/backoff и ограничивает pagination;
  failure обязательной части sync не публикует успешную readiness.
- Bot entrypoint проверяет proxy path, перезапускает child process только для
  определённого retry exit code и поддерживает freshness readiness heartbeat.
- Reporter применяет service-key authentication, body/request/concurrency
  limits и не наследует Telegram proxy для internal calls.
- Bot delivery leases и daily job ledger позволяют восстанавливать зависшие
  попытки без слепой повторной отправки.
- Xray выполняет startup/runtime probes и переключает primary/fallback route.

## Источники истины

При расхождении документации и реализации приоритет проверки текущего состояния
такой:

1. `migrations/`, `database/schema_manifest.py` и versioned schemas — структура
   и форматы данных;
2. `pyproject.toml`, Dockerfiles и Compose — entrypoints и deployment topology;
3. package imports, runtime composition и tests — зависимости и поведение;
4. `docs/contracts/` — опубликованные интерфейсы и compatibility policy;
5. этот документ — обзор текущей системы.

Изменение runtime boundary, writer ownership, сетевого маршрута или публичного
контракта требует синхронного обновления соответствующего source of truth,
контрактных тестов, этой архитектуры и [карты проекта](PROJECT_MAP.md).
