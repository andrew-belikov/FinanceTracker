# Целевые контракты FinanceTracker

Статус: принято как целевое состояние, внедрение не завершено

Дата решения: 2026-09-15

## Назначение и терминология

Документ определяет устойчивые границы, которые должны переживать внутренний
рефакторинг. Он не утверждает, что все требования уже реализованы. Текущее
поведение до переключения описано в [`BEHAVIOR.md`](BEHAVIOR.md), целевая
структура — в [`TARGET_ARCHITECTURE.md`](TARGET_ARCHITECTURE.md).

Термины:

- **обязан** — после перевода соответствующего раздела в `enforced` нарушение
  блокирует merge или readiness; до этого оно блокирует завершение связанного
  milestone и финальное слияние ветки `refactor`;
- **следует** — отклонение требует зафиксированного решения и срока устранения;
- **может** — допустимая реализация без изменения контракта;
- **instant** — однозначный момент времени;
- **civil date** — календарная дата в явно указанной IANA timezone;
- **artifact** — пользовательский файл или versioned machine-readable export;
- **delivery** — попытка отправки внешнему получателю;
- **schema manifest version** — монотонная версия декларативного manifest;
- **schema manifest digest** — SHA-256 canonical manifest и ordered required
  migration set `(filename, checksum)`.

Lifecycle каждого контракта:

| Статус | Значение |
| --- | --- |
| `proposed` | решение обсуждается и не является gate |
| `adopted` | целевое решение принято и блокирует завершение milestone |
| `enforced` | существует executable check; нарушение блокирует merge/readiness |

Все разделы этого документа имеют статус `adopted`, пока отдельный атомарный
коммит не добавит реализацию и проверку и не отметит раздел `enforced`. Текущие
отклонения учитываются в `PROJECT_AUDIT.md` и не делают существующий production
автоматически невалидным.

## Совместимость и versioning

### Общие правила

- Публичный или межпроцессный контракт имеет стабильный идентификатор версии.
- Reader обязан отвергать неизвестную major version.
- Добавление optional-поля без изменения смысла совместимо внутри major version.
- Удаление, rename, изменение типа, единицы, nullability или семантики требует
  новой major version.
- Изменение default, влияющее на финансовый результат или доставку, считается
  behavior change и отражается в changelog.
- Adapter-specific типы T-Invest, Telegram и Ollama не входят в domain или
  reporting contracts.

Идентификаторы release:

- `application_version` — release SemVer плюс точный source commit SHA;
- `config_schema_version` — монотонное целое число, записанное в image label и
  release manifest; оно меняется при несовместимом изменении config contract;
- `schema_manifest_version` — монотонное целое число manifest;
- `schema_manifest_digest` — SHA-256 canonical manifest и ordered migration set.

### Поддерживаемое окно

- Runtime обязан поддерживать чистую установку и обновление с предыдущей
  опубликованной production schema `N-1`.
- Более старые схемы обновляются только последовательностью опубликованных
  миграций.
- Каждый application image публикует `min_schema_manifest` и
  `max_schema_manifest`; readiness разрешён только внутри этого диапазона и при
  наличии всех required migrations image. Дополнительные additive migrations
  допустимы, если входят в заявленный диапазон совместимости.
- Migrator после обновления требует exact target manifest version/digest.
- Для несовместимого изменения применяется expand → backfill → switch → contract;
  contract-фаза выполняется не раньше следующего release window.

### Versioned artifacts

Независимо версионируются:

- существующий `monthly_report_payload.v1` на время перехода;
- целевой расширенный `monthly_report_payload.v2`;
- `monthly_ai_input.v1`;
- `monthly_ai_output.v1`;
- существующий `monthly_report_artifact.v1` на время перехода;
- целевой расширенный `monthly_report_artifact.v2`;
- `monthly_fallback_narrative.v1`;
- `dataset_version=3` и последующие major versions;
- internal reporter HTTP request/error contract;
- structured logging schema и event catalog;
- release manifest.

## Конфигурация

### Источник и ownership

Каждая настройка имеет одного owner-service и документированные:

- имя;
- тип и допустимый диапазон;
- обязательность и default;
- secret/non-secret classification;
- startup validation;
- reload policy;
- поведение при несовместимости;
- deprecation lifecycle.

`.env.example`, Compose, runtime settings и `CONFIG.md` обязаны проверяться
автоматическим parity test.

### Startup validation

Production-процесс обязан завершиться до readiness при:

- отсутствии обязательных credentials;
- пустом или невалидном `ALLOWED_USER_IDS`;
- невалидной IANA timezone;
- невалидном диапазоне timeout/concurrency/retention;
- неподдерживаемой schema manifest version/digest;
- `VERIFY_SSL=false` в любом production-профиле;
- доступе к секрету, который не принадлежит runtime-роли.

Валидация выполняется явно в composition root. Импорт Python-модуля не читает
обязательные secrets и не создаёт DB/HTTP clients.

Отключение TLS verification разрешено только технически изолированному test
profile с отдельным test-only флагом. Production break-glass для отключения TLS
не существует.

### Secrets

- Целевой интерфейс секретов — per-service read-only files и переменные
  `*_FILE`.
- Значение секрета не передаётся в logs, Compose output, image labels, release
  manifest, CI artifacts или exception message.
- Telegram token доступен только bot.
- T-Invest token доступен только tracker.
- VLESS configuration доступна только xray-client.
- Reporter key доступен только bot и reporter.
- DB credential соответствует отдельной роли migrator/tracker/bot/reporter.
- Ротация каждого класса секрета описывается runbook.

## Время и календарные интервалы

### Модель времени

- Instant представлен timezone-aware UTC в Python.
- Целевой PostgreSQL type для instant — `TIMESTAMPTZ`.
- Civil date хранится как `DATE` и интерпретируется только вместе с явно
  указанной IANA timezone.
- Все временные интервалы задаются как полуинтервалы `[start, end)`.
- Legacy `TIMESTAMP WITHOUT TIME ZONE` трактуется как UTC только по явному
  migration rule.
- Наивный `datetime` не пересекает новую application/domain boundary.

### Reporting и schedule

- `TIMEZONE` определяет civil reporting boundaries.
- Отдельная timezone расписания допустима только как явно названная настройка с
  owner и тестами DST.
- Несуществующее или неоднозначное локальное время обрабатывается по
  документированной политике scheduler, а не по timezone хоста.
- `datetime.utcnow()` в production-коде запрещён после завершения migration.

## Деньги, количество и валюта

- Денежные суммы, quantity, rates и проценты вычисляются через `Decimal`.
- `float` запрещён между ingestion boundary и presentation boundary.
- T-Invest `units/nano` преобразуются непосредственно в `Decimal`.
- Precision и scale хранилища являются частью schema contract.
- Ingestion сохраняет исходную decimal precision до ограничения фактическим
  schema type; binary-float rounding запрещён.
- Derived calculations используют `Decimal` context precision не ниже 28 и не
  округляются для промежуточных сравнений.
- Presentation amounts округляются до minor unit валюты через
  `ROUND_HALF_UP`, если отдельный продуктовый контракт не задаёт иное.
- Quantity квантуется только по instrument precision; проценты округляются до
  `0.01` percentage point только при presentation/export boundary.
- Каждая дополнительная quantization boundary и rounding mode фиксируются рядом
  с показателем и проверяются golden test.
- Валюта нормализуется в uppercase ISO 4217; неизвестная валюта представляется
  явным sentinel `UNKNOWN` и не маскируется под RUB.
- Суммы разных валют не складываются без явного FX source, rate, timestamp и
  rounding policy.
- Отсутствие FX policy приводит к раздельному представлению валют или fail-fast.

## PostgreSQL и схема

### Источник истины

Versioned SQL migrations являются каноническим механизмом изменения production
schema. Декларативный machine-readable schema manifest описывает ожидаемое
конечное состояние. ORM обязан соответствовать manifest, но не создаёт
production baseline через `Base.metadata.create_all()`.

Schema manifest имеет монотонный `schema_manifest_version`; digest вычисляется
как SHA-256 canonical manifest вместе с ordered required migration set. Manifest
включает:

- tables и columns;
- PostgreSQL types, precision и scale;
- nullability и defaults;
- primary/unique/foreign keys;
- checks;
- indexes и predicates;
- views;
- required extensions;
- ownership и grants.

`migrate --check` обязан сравнивать фактическую схему с manifest, а не только
проверять наличие таблиц.

### Ключевые идентичности

Минимально сохраняются следующие уникальности:

- operation: `(account_id, operation_id)`;
- portfolio snapshot: `(account_id, snapshot_date)`;
- income event: `(account_id, figi, event_date, event_type, currency)`, где
  `event_date` является local civil date;
- notification delivery:
  `(notification_kind, notification_key, chat_id, message_type)`.

Изменение идентичности требует отдельной миграции, collision preflight и
production-derived acceptance.

### DB-роли

| Роль | Разрешения |
| --- | --- |
| migrator | DDL, grants, migration ledger |
| tracker | DML ingestion-owned tables, необходимое чтение |
| bot | чтение, job/delivery ledgers, `rebalance_targets`, разрешённые manual поля `operations` и notification state |
| reporter | read-only reporting tables/views |

Права проверяются positive и negative integration tests.

## Миграции

- Миграции forward-only и append-only.
- Filename и bytes применённой миграции неизменяемы.
- Исправление выпускается новой миграцией.
- Advisory lock сериализует migrators.
- Одна миграция и её ledger insert выполняются в одной транзакции.
- Destructive или неоднозначная конверсия обязана fail closed.
- Каждая миграция определяет preconditions, postconditions, compatibility window,
  backup requirement и recovery procedure.
- Metadata старых и новых миграций хранится в immutable sidecar registry,
  ключованном filename и checksum; применённые SQL-файлы ради metadata не
  редактируются.
- Writer не стартует до успешного завершения migrator.
- Application rollback не означает schema rollback.
- Необратимое восстановление выполняется только через break-glass restore в
  новый volume с явной оценкой потери данных относительно RPO.

## Транзакции и idempotency

### Общие правила

- Application use case владеет транзакционной границей.
- Repository не выполняет скрытый `commit`, если он не реализует полный
  автономный use case.
- Network call не удерживает открытую DB transaction.
- Partial success либо запрещён, либо явно моделируется устойчивым ledger.
- Повторный запуск после crash обязан быть безопасным согласно контракту use
  case.

### Ingestion

- Snapshot и operation sync могут быть разными транзакциями, но их
  согласованность и допустимое окно рассинхронизации документируются.
- Все страницы одного operation sync используют зафиксированную верхнюю границу
  `to` и сначала записываются в durable staging batch с `sync_run_id` короткими
  транзакциями.
- Cursor loop, отсутствующий cursor, превышение page limit или невалидный
  payload помечают staging batch как failed; он не виден основным readers.
- После получения и валидации всех страниц одна DB-транзакция атомарно публикует
  batch в основные таблицы и фиксирует terminal sync state.
- Network call не выполняется внутри publish transaction; незавершённые staging
  batches имеют bounded retention и безопасно очищаются или продолжаются.
- Upsert сохраняет пользовательские manual fields, если внешний источник ими не
  владеет.

### Scheduled jobs

- Job claim использует reclaimable lease и fencing token.
- Только текущий owner может heartbeat, complete или release lease.
- `completed` является терминальным состоянием.
- Просроченный lease может быть захвачен новым owner без права старого owner
  завершить run.

### Telegram delivery

Exactly-once доставка внешней системе не обещается. Контракт:

- deduplication ведётся на получателя и message type;
- `sent` — подтверждённое терминальное состояние;
- transport ambiguity записывается как `uncertain`;
- `uncertain` автоматически не повторяется;
- ручное разрешение `uncertain` является audit-событием;
- plain-text fallback выполняется один раз только при однозначной Markdown parse
  error, но не при network timeout.

## Reporting

### Pipeline

```text
request -> reporting repository -> canonical payload
        -> deterministic facts -> optional narrative -> renderer -> artifact
```

- Reporting repository имеет read-only DB session.
- Canonical payload не содержит SQLAlchemy или Telegram objects.
- Renderer является чистым потребителем payload и не обращается к БД.
- AI narrative не вычисляет и не изменяет финансовые значения.
- AI input ограничен по размеру и содержит только нормализованные данные.
- AI output проходит structural и semantic validation.
- Допускается не более одной repair-попытки.
- Ошибка или отсутствие Ollama всегда приводит к deterministic fallback.

### `monthly_report_payload.v2`

Payload обязан содержать или однозначно связывать:

- requested и effective period;
- reporting timezone и currency policy;
- account identity без credentials;
- source snapshot IDs/count и data-quality flags;
- schema manifest version/digest и application version;
- deterministic financial facts;
- canonical payload digest.

Один payload строится внутри одной read-only `REPEATABLE READ` транзакции с
явным `as_of`/source snapshot cutoff.

Canonical JSON кодируется UTF-8, использует сортировку keys, separators `,` и
`:`, decimal strings без exponent и ISO 8601 для date/time. Digest — SHA-256
этих bytes. Из canonical form исключаются только перечисленные schema paths:
`generated_at_utc`, временные пути, PDF metadata и compatibility-поле
`meta.has_ai_narrative`. Narrative source относится к artifact metadata и не
может менять digest deterministic payload.

Существующий `v1` сохраняется неизменным на compatibility window. Producer
может добавить только optional extension metadata, которую старый reader
игнорирует; обязательная расширенная форма публикуется только как `v2`.

### `monthly_report_artifact.v2`

Artifact metadata включает:

- payload digest;
- renderer version;
- narrative source: `ollama` или `deterministic`;
- application version и schema manifest version/digest;
- period;
- content type и безопасное filename;
- artifact checksum.

Существующий `monthly_report_artifact.v1` не переопределяется; новые обязательные
metadata появляются только в `v2`.

## Internal reporter HTTP API

Контракт имеет идентификатор `reporter_http.v1`. Сохраняется текущая service
boundary:

- `GET /healthz` — bounded readiness payload без финансовых данных;
- `POST /reports/monthly/pdf` — запрос monthly artifact;
- авторизация — `X-Reporter-Service-Key` с constant-time comparison;
- request body — JSON object с optional `year` и `month`;
- body size, socket timeout, build timeout и concurrency ограничены;
- success — `application/pdf`, `Content-Disposition`, `X-Report-Period`;
- errors — JSON с versioned stable error code;
- overload и build timeout возвращают retryable `503`;
- invalid request является permanent `400`;
- missing/invalid authentication возвращает `401/403`;
- endpoint не публикуется на host и не наследует Telegram proxy.

Клиент передаёт `X-Reporter-Contract-Version: 1`; server возвращает этот header
в success и error responses. Error envelope:

```json
{
  "contract": "reporter_http.v1",
  "status": "error",
  "error": "stable_error_code",
  "message": "safe optional detail",
  "request_id": "optional correlation id"
}
```

Переход совместим: один release server принимает отсутствующий header как `v1`,
следующий release делает header обязательным. Неизвестная major version
отклоняется до выполнения report build стабильной ошибкой
`unsupported_contract_version`.

Изменение path, header, request semantics или error code требует новой версии
контракта либо совместимого переходного окна.

## Внешние интеграции

### T-Invest

- Разрешены только read-only `Get*` RPC.
- Retry допускается для transport errors, `408`, `429` и `5xx`.
- Backoff ограничен, использует jitter и уважает bounded `Retry-After`/rate-limit
  hints.
- Вся операция имеет общий deadline, а не только timeout одной попытки.
- Mutation RPC запрещён без отдельного утверждённого idempotency и safety
  contract.
- Account выбирается по exact ID либо как единственный открытый account;
  неоднозначность приводит к fail-fast.
- Неожиданная форма payload не превращается в нулевые финансовые значения.

### Telegram

- Команды доступны только allowlisted user в private chat.
- Неавторизованный запрос не раскрывает наличие данных или конфигурации.
- Telegram SDK types остаются внутри transport/handlers.
- Финансовые use cases возвращают presentation-neutral result.
- Proxy используется только для Telegram traffic.
- Internal reporter и PostgreSQL всегда обходят Telegram proxy.

### Ollama

- Интеграция optional и не блокирует deterministic PDF.
- Модель не получает credentials, raw logs или ненужные идентификаторы.
- Output модели считается недоверенным до validation.
- Ollama profile может быть полностью выключен без внешней network dependency.

### Xray

- SOCKS endpoint доступен только bot через internal network.
- Xray не имеет доступа к DB, reporter или Ollama network.
- VLESS configuration не попадает в logs, image или health payload.
- Отключённый proxy остаётся healthy в явно определённом idle-режиме.

## Расписание и DST

- Nonexistent local time в spring-forward сдвигается к первому валидному
  instant после gap.
- Ambiguous local time в fall-back выполняется ровно один раз на первом fold.
- Каждый scheduled job имеет stable logical run key в civil timezone и
  idempotency ledger, поэтому restart не создаёт второй запуск.
- Startup catch-up выполняет пропущенный run не более одного раза, если его
  logical key не terminal и возраст не превышает
  `JOB_CATCHUP_MAX_AGE_SECONDS` с default 6 часов.
- Run старше catch-up window помечается `missed` и требует отдельной operator
  policy; он не запускается молча.

## Ошибки и повторные попытки

Единая taxonomy:

| Класс | Retry | Действие |
| --- | --- | --- |
| `configuration_permanent` | нет | завершить до readiness |
| `validation_permanent` | нет | отклонить вход |
| `data_integrity_manual` | нет | остановить сценарий, operator action |
| `upstream_transient` | bounded | backoff + deadline |
| `delivery_ambiguous` | нет автоматически | записать `uncertain` |
| `overload_retryable` | bounded caller retry | backpressure |
| `internal_bug` | нет до нового deploy | error event + unhealthy/degraded |

Для каждого retryable use case документируются owner повтора, max attempts,
deadline, backoff/jitter, idempotency prerequisite и terminal state. Бесконечные
внутрипроцессные retry loops запрещены.

## Observability и privacy

- First-party logs — JSON Lines.
- `event` задаётся явно, стабилен и использует `snake_case`.
- Logging schema и event catalog имеют version.
- Correlation IDs передаются через application, job и internal HTTP boundaries.
- Health отражает пользовательски значимую готовность, а не только живой PID.
- Secret, auth header, VLESS link, financial payload и raw upstream body не
  попадают в logs.
- Exception logging сохраняет тип, безопасное сообщение и stack trace без
  чувствительных полей.
- Каждый alert связывается с SLI и runbook.

Целевые начальные SLO вступают в силу только после появления соответствующих
SLI и минимум одного полного окна наблюдения:

- availability bot/tracker: доля минут с успешным readiness и независимым
  upstream probe, цель `99.5%` за календарный месяц;
- snapshot freshness: доля минут, когда последний успешный snapshot не старше
  `max(2 × SNAPSHOT_INTERVAL, 15 минут)`, цель `99%`;
- scheduled delivery: доля intended `(recipient, message_type)` со статусом
  `sent` в пределах 10 минут, цель `99%`; `uncertain` считается failure и
  измеряется отдельно;
- readiness после обычного deploy: до 5 минут;
- PDF: доля запросов с artifact или deterministic fallback в configured 180
  секунд, цель `99%`; при менее чем 20 запросах за окно публикуется только raw
  count без процентного SLO.

Planned maintenance исключается только если объявлен до начала, имеет owner и
ограничен суммарно четырьмя часами за месяц. Upstream outage исключается только
по независимому probe с того же host/network path; отсутствие такого
доказательства остаётся downtime FinanceTracker.

Целевой RPO — 15 минут после внедрения WAL archival; до этого честно заявляется
не лучше периода проверенного backup. Целевой RTO — 60 минут после
автоматизации restore. До трёх успешных измеренных restore drills это objectives,
а не подтверждённые показатели; затем публикуется фактический percentile и
худший результат.

## Build, release и deploy

- CI строит images один раз и публикует их по digest.
- Base images закреплены digest.
- Каждый image имеет SBOM, provenance и vulnerability result.
- Подписанный pre-deploy release manifest связывает source SHA, image digests,
  SBOM/provenance digests и CI run. Trust policy fail closed проверяет issuer,
  signer identity, repository, workflow, ref и разрешённый registry.
- Production runner сначала проверяет manifest/signatures и получает candidate
  и previous images, затем выполняет image/schema preflight, backup gate,
  exact-digest migration и запуск заранее собранных images.
- Running image digest обязан совпадать с release manifest.
- Deployable unit содержит source SHA, image digests, application version,
  schema manifest version/digest, config schema version и CI run identity.
- `main` защищена required checks; force-push и удаление запрещены.
- Writer не запускается до успешной migration.
- Post-deploy acceptance создаёт отдельный immutable receipt без секретов и со
  ссылкой на pre-deploy release manifest.

## Backup, restore и rollback

- Перед deploy создаётся no-clobber `pg_dump --format=custom`.
- Manifest содержит source SHA, image digests, PostgreSQL version, migration
  ledger/checksums и timestamp.
- Dump и manifest имеют SHA-256 и хранятся зашифрованно вне checkout и Docker
  volume; минимум одна копия off-host.
- Для достижения RPO 15 минут используются периодический physical base backup и
  непрерывная WAL chain с off-host replication. Контролируются timeline,
  continuity, retention, encryption и отдельная сохранность ключа; PITR drill
  восстанавливает заданный recovery target time.
- Backup считается подтверждённым только после restore в disposable PostgreSQL,
  schema probes и `migrate --check`.
- Release manifest хранит предыдущие image digests и schema compatibility.
- Application rollback разрешён только при доказанной совместимости schema.
- При несовместимой необратимой migration writers остаются остановлены, restore
  полностью выполняется в новый volume и проходит checksum, schema и application
  probes до переключения production pointer. Переключение атомарно, требует
  break-glass approval и post-switch acceptance; старый volume не изменяется и
  сохраняется на установленный retention period.
- `docker compose down -v` запрещён operational policy.
- Restore drill выполняется автоматически ежемесячно и оператором ежеквартально.

## Уровни тестирования

### L0 — static и contracts

- config parity;
- migration byte identity;
- schema и JSON schemas;
- dependency/import boundaries;
- secret scan;
- lint, types, ShellCheck и Dockerfile checks.

### L1 — unit

- Decimal math и rounding;
- timezone/DST и `[start, end)`;
- currency isolation;
- payload canonicalization;
- retry classification;
- authorization и presentation без внешней сети.

### L2 — PostgreSQL 16 integration

- чистая установка;
- upgrade с `N-1`;
- повторный idempotent migration run;
- конкурентные migrators;
- rollback текущей migration при ошибке;
- полное сравнение schema manifest;
- реальные `AT TIME ZONE`, `ON CONFLICT`, locks, leases и reporting queries;
- positive/negative DB grants.

### L3 — adapter integration

Локальные deterministic fake servers проверяют T-Invest, Telegram и Ollama:
timeouts, `429/5xx`, malformed JSON, repeated cursor, retry deadline и ambiguous
delivery.

### L4 — Compose smoke

- build всех images;
- `migrate` завершается с кодом `0`;
- health/readiness всех включённых сервисов;
- reporter auth, invalid request, overload и timeout;
- proxy и Ollama в enabled/disabled profiles;
- restart recovery и network isolation.

### L5 — production-derived acceptance

Bundle обязан:

- быть свежим, находиться вне Git/CI и иметь checksum, source SHA, PostgreSQL
  version и migration ledger;
- фиксировать baseline executable/source SHA и candidate SHA;
- восстанавливаться в отдельный temporary volume/project/network;
- использовать заглушки secrets и технически заблокированный outbound;
- проходить migration, повторную migration и `migrate --check`;
- проходить uniqueness/FK/currency/time normalization probes;
- запускать baseline и candidate против независимых восстановлений одного и того
  же immutable bundle;
- сравнивать canonical monthly payload, dataset, TWR, P&L, cashflows, income и
  rebalance с фиксированными правилами ordering, `None`, Decimal strings,
  rounding и списком volatile paths;
- подтверждать минимальное покрытие bundle: полный отчётный месяц, минимум два
  snapshot boundary, executed operations, cashflow, income и notification/job
  ledger; при отсутствии класса данных gate помечается `not_covered`, а не
  `passed`;
- проверять delivery ledger через crash-before-send, ambiguous-send и restart
  без реальных сообщений;
- создавать обезличенный acceptance receipt с hashes, versions и результатами.

После проверки временная БД удаляется. Необъяснённое расхождение блокирует merge.

### L6 — live canary

Выполняется только после отдельного разрешения на deploy. Live canary не
заменяет уровни L0–L5.

## Definition of Done контрактов

Контракты считаются внедрёнными, когда:

- для них существуют executable checks или machine-readable schemas;
- код, Compose, CI и active docs не расходятся;
- негативные сценарии проверены на соответствующем уровне;
- compatibility window и migration/recovery path документированы;
- production-derived acceptance не выявляет необъяснённых расхождений;
- каждое намеренное изменение контракта имеет отдельный changelog/ADR и
  атомарный коммит.
