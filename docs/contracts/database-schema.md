# Схема PostgreSQL

- **Contract ID / версия:** `schema_manifest.v2`
- **Статус:** active
- **Владелец:** `financetracker.database`
- **Writers:** `tracker`, migration runner, bot delivery/lease repositories
- **Readers:** `bot`, `reporter`, health/readiness checks
- **Исполняемый контракт:** `src/financetracker/database/schema_manifest.py`
- **DDL source:** ordered forward migrations in `migrations/`

## Владение и namespace

Приложение владеет перечисленными ниже relations в PostgreSQL schema `public`.
Все temporal instants хранятся как `TIMESTAMPTZ`; календарные даты — как
`DATE`. Денежные и количественные значения хранятся в `NUMERIC`, а не float.
Обязательные идентификаторы account/operation/instrument не нормализуются БД:
их семантика принадлежит producer.

Обозначения в каталоге: `!` — `NOT NULL`, `?` — nullable, `PK` — primary key,
`UQ` — unique constraint, `IX` — index, `FK` — foreign key.

## Каталог relations

### `schema_migrations`

Ledger применённых migration: `filename text ! PK`, `checksum_sha256 text !`,
`applied_at timestamptz ! default now()`. Файл после применения immutable;
несовпадение checksum или исчезновение файла блокирует migration/check.

### `instruments`

Справочник инструментов: `id integer ! PK default nextval`, `figi text !`,
`ticker text ?`, `name text ?`, `class_code text ?`, `instrument_type text ?`,
`created_at timestamptz ! default now()`. `UQ instruments_figi_key(figi)`.

### `portfolio_snapshots`

Дневные состояния счёта: `id integer ! PK default nextval`, `account_id text !`,
`account_name text ?`, `snapshot_at timestamptz !`, `snapshot_date date !`,
`currency text !`, `total_value numeric(18,2) ?`, `total_shares numeric(18,2) ?`,
`total_bonds numeric(18,2) ?`, `total_etf numeric(18,2) ?`,
`total_currencies numeric(18,2) ?`, `total_futures numeric(18,2) ?`,
`expected_yield numeric(18,2) ?`, `expected_yield_pct numeric(9,4) ?`,
`created_at timestamptz ! default now()`.

- `UQ uq_snapshot_account_date(account_id, snapshot_date)` обеспечивает один
  актуальный snapshot на локальную дату счёта.
- `IX ix_portfolio_snapshots_snapshot_at(snapshot_at)`.
- `IX ix_portfolio_snapshots_snapshot_date(snapshot_date)`.

### `portfolio_positions`

Состав snapshot: `id integer ! PK default nextval`, `snapshot_id integer !`,
`figi text !`, `instrument_id integer ?`, `instrument_uid text ?`,
`position_uid text ?`, `asset_uid text ?`, `ticker text ?`, `name text ?`,
`instrument_type text ?`, `quantity numeric(18,6) ?`, `currency text ?`,
`current_price numeric(18,4) ?`, `current_nkd numeric ?`,
`position_value numeric(18,2) ?`, `expected_yield numeric(18,2) ?`,
`expected_yield_pct numeric(9,4) ?`, `weight_pct numeric(9,4) ?`,
`raw_payload_json text ?`, `created_at timestamptz ! default now()`.

- `FK portfolio_positions_snapshot_id_fkey(snapshot_id) → portfolio_snapshots(id)`.
- `FK portfolio_positions_instrument_id_fkey(instrument_id) → instruments(id)`.
- ORM удаляет positions каскадно вместе с parent snapshot; DB manifest не
  обещает отдельный `ON DELETE` contract.

### `operations`

Broker operations: `id bigint ! PK default nextval`, `account_id text !`,
`operation_id text !`, `operation_type text ! default operation_type_input`,
`date timestamptz !`, `amount numeric(18,2) !`, `currency text !`,
`description text ?`, `source text ?`, `instrument_uid text ?`, `figi text ?`,
`cursor text ?`, `broker_account_id text ?`, `parent_operation_id text ?`,
`name text ?`, `state text ?`, `instrument_type text ?`, `instrument_kind text ?`,
`position_uid text ?`, `asset_uid text ?`, `price numeric ?`,
`commission numeric ?`, `yield numeric ?`, `yield_relative numeric ?`,
`accrued_int numeric ?`, `quantity bigint ?`, `quantity_rest bigint ?`,
`quantity_done bigint ?`, `cancel_date_time timestamptz ?`,
`cancel_reason text ?`, `cashflow_category text ?`,
`created_at timestamptz ! default now()`.

- `UQ uq_operations_account_operation(account_id, operation_id)` — identity
  операции scoped to account.
- `IX ix_operations_cashflow_category(account_id, cashflow_category, date)`.
- `cashflow_category='iis_tax_deduction'` — единственное активное ручное
  значение; broker upsert MUST сохранять ручную классификацию.
- Financial readers используют deduplicated operation view/CTE и только
  `state='OPERATION_STATE_EXECUTED'`, когда это требуется расчётом.

### `income_events`

События дохода: `id integer ! PK default nextval`,
`account_id varchar !`, `figi varchar !`, `event_date date !`,
`event_type varchar !`, `gross_amount numeric(18,2) !`,
`tax_amount numeric(18,2) !`, `net_amount numeric(18,2) !`,
`net_yield_pct numeric(9,4) !`, `notified boolean ! default false`,
`created_at timestamptz ! default now()`, `currency text !`.

`UQ uq_income_events_account_figi_date_type_currency(account_id, figi,
event_date, event_type, currency)` не допускает смешения валют в identity.
Неизвестная валюта записывается явным sentinel `UNKNOWN`, не `NULL`.

### `asset_aliases`

Нормализация identity: `id bigint ! PK default nextval`, `asset_uid text !`,
`instrument_uid text ?`, `figi text ?`, `ticker text ?`, `name text ?`,
`first_seen_at timestamptz !`, `last_seen_at timestamptz !`,
`created_at timestamptz ! default now()`, `updated_at timestamptz ! default now()`.

- `UQ uq_asset_aliases_asset_instrument_figi(asset_uid, instrument_uid, figi)`.
- `IX ix_asset_aliases_asset_uid(asset_uid)`,
  `ix_asset_aliases_instrument_uid(instrument_uid)`,
  `ix_asset_aliases_figi(figi)`.

### `rebalance_targets`

Цели распределения: `id integer ! PK default nextval`, `account_id text !`,
`asset_class text !`, `target_weight_pct numeric(9,4) !`,
`created_at timestamptz ! default now()`, `updated_at timestamptz ! default now()`.
`UQ uq_rebalance_targets_account_class(account_id, asset_class)`.

Активные classes: `stocks`, `bonds`, `etf`, `currency`. Допустимый диапазон и
сумма целей проверяются application layer, не DB constraint.

### `invest_notifications`

Дедупликация уведомлений о пополнении: `id integer ! PK default nextval`,
`account_id text !`, `operation_id text !`, `operation_date timestamptz !`,
`amount numeric(18,2) !`, `created_at timestamptz ! default now()`.
`UQ uq_invest_notifications_account_operation(account_id, operation_id)`.

### `bot_daily_job_runs`

Lease плановой задачи: `id integer ! PK default nextval`, `job_name text !`,
`run_date date !`, `status text ! default started`, `attempt_id text !`,
`claimed_at timestamptz !`, `heartbeat_at timestamptz !`,
`completed_at timestamptz ?`, `sent_total integer ?`, `failed_total integer ?`,
`created_at timestamptz ! default now()`.
`UQ uq_bot_daily_job_runs_job_date(job_name, run_date)`.

`attempt_id` fences старого owner после takeover; `completed` терминален.

### `bot_notification_deliveries`

Доставка по recipient/message: `id integer ! PK default nextval`,
`notification_kind text !`, `notification_key text !`, `chat_id bigint !`,
`message_type text !`, `status text ! default started`, `attempt_id text !`,
`claimed_at timestamptz !`, `delivered_at timestamptz ?`,
`created_at timestamptz ! default now()`, `updated_at timestamptz ! default now()`.

- `UQ uq_bot_notification_deliveries_identity(notification_kind,
  notification_key, chat_id, message_type)`.
- `IX ix_bot_notification_deliveries_status_claimed(status, claimed_at)`.
- Неоднозначный transport result сохраняется как `uncertain` и не должен
  автоматически переотправляться.

### `payout_calendar_events`

Ожидаемые выплаты: `id integer ! PK default nextval`, `account_id text !`,
`figi text !`, `event_type text !`, `event_uid text !`, `payment_date date !`,
`quantity numeric(18,6) !`, `fetched_at timestamptz !`,
`instrument_uid text ?`, `ticker text ?`, `name text ?`,
`instrument_type text ?`, `record_date date ?`, `last_buy_date date ?`,
`amount_per_unit numeric(18,9) ?`, `expected_amount numeric(18,2) ?`,
`currency text ?`, `source_event_type text ?`, `coupon_start_date date ?`,
`coupon_end_date date ?`, `coupon_period_days integer ?`,
`created_at timestamptz ! default now()`, `updated_at timestamptz ! default now()`.

- `UQ uq_payout_calendar_event_source(account_id, figi, event_type, event_uid)`.
- `IX ix_payout_calendar_events_payment_date(payment_date)`.
- `IX ix_payout_calendar_events_account_payment(account_id, payment_date)`.

### View `deposits`

Read-only compatibility view для исторической схемы. Активные readers MUST
использовать `operations`. Manifest v2 проверяет существование relation с
`relkind='v'`, но пока не проверяет SQL definition view; это осознанная граница
текущей автоматической гарантии, а не разрешение менять definition произвольно.

## Migration contract

Forward migrations применяются лексикографически; файлы `*.rollback.sql` и
macOS resource-fork `._*` исключены. Актуальный ordered set:

1. `20260220_portfolio_baseline.sql`
2. `20260221_operations_from_deposits.sql`
3. `20260225_operations_add_instrument_columns.sql`
4. `20260226_income_events.sql`
5. `20260304_operations_operation_item_fields.sql`
6. `20260324_dataset_source_fields.sql`
7. `20260324_rebalance_targets_and_invest_notifications.sql`
8. `20260404_bot_daily_job_runs.sql`
9. `20260728_payout_calendar_events.sql`
10. `20260729_coupon_annualized_period.sql`
11. `20260805_operations_cashflow_category.sql`
12. `20260814_bot_notification_delivery_leases.sql`
13. `20260814_data_identity_and_currency.sql`
14. `20260915_currency_unknown_sentinel.sql`
15. `20260915_schema_contract_alignment.sql`
16. `20260915_timezone_aware_utc.sql`

Одна migration и запись ledger коммитятся атомарно под PostgreSQL advisory lock
`1731904221`. Session-local timezone берётся из `TIMEZONE`, затем legacy
`SCHED_TZ`, затем `Europe/Moscow`.

## Drift и failure semantics

`migrate --check` работает read-only и fail-closed при отсутствии ledger,
missing/pending/modified migration, отсутствующей ORM table либо расхождении
manifest по ожидаемым column type/nullability/default, PK, named UQ/IX/FK и
наличию view.

Текущий manifest проверяет обязательное подмножество и **не** отвергает лишние
columns, constraints, indexes или tables; SQL definition `deposits` также не
digest-ится. Поэтому успешный check доказывает присутствие ожидаемого contract,
но не полное отсутствие всех ручных расширений.

## Совместимость

Additive nullable column/table/index в forward migration совместимы при условии,
что старый runtime их игнорирует. `NOT NULL`, rename/drop, изменение типа,
timezone/precision, identity key или uniqueness требуют staged rollout по
[политике версий](versioning.md). Поддерживаемый Compose deployment всегда
проходит migration service до application containers. Tracker сохраняет
совместимый вызов ORM `create_all` после этого gate, но он не добавляет/меняет
существующие columns и не является заменой migration path.

## Проверка

```bash
python -m unittest tests.test_tracker_migrations \
  tests.test_migration_byte_identity tests.test_database_infrastructure
FINANCETRACKER_POSTGRES_INTEGRATION=1 python -m unittest \
  tests.test_postgresql_integration
```

Integration check требует отдельный PostgreSQL 16 и не запускается по умолчанию.
