# Monthly report payload

- **Contract ID / версия:** `monthly_report_payload.v1`
- **Статус:** active, internal
- **Владелец / producer:** `financetracker.reporting.report_payload`
- **Consumers:** HTML/PDF renderer, narrative/AI input builder, report pipeline
- **Машиночитаемый контракт:**
  [monthly-report-payload.schema.json](monthly-report-payload.schema.json)

## Назначение и граница

Payload — сериализованный детерминированный снимок фактов monthly report. Он
строится reporter напрямую из read-only PostgreSQL queries и не передаётся bot
по HTTP: bot получает готовый PDF. Renderer MUST NOT открывать DB session.
Narrative получает производный bounded `monthly_ai_input.v1`; AI output не
изменяет financial facts payload.

## Представление данных

- Все monetary, quantity, percent и другие `Decimal` сериализуются JSON strings
  в fixed-point форме без потери precision. Consumer MUST NOT использовать
  binary float для финансовых вычислений.
- Calendar dates — ISO `YYYY-MM-DD`; instants — ISO 8601 timezone-aware strings.
- `generated_at_utc` — UTC instant; локальные границы периода вычисляются в
  `meta.timezone` и запрашиваются как полуинтервал `[start, end_exclusive)`.
- Валюта — uppercase currency code или sentinel `UNKNOWN`. Номиналы разных
  валют MUST NOT суммироваться без явного FX contract; разбивка сохраняется в
  `income_by_currency` и `operation_cashflows_by_currency`.
- Asset identity выбирается в порядке стабильных UID/alias данных; поле
  `logical_asset_id` — ключ агрегации внутри payload, не новый broker ID.

## Обязательные sections

| Section | Семантика |
|---|---|
| `schema_version` | всегда `monthly_report_payload.v1` |
| `meta` | account, timezone/currency, период, provenance snapshot и generation time |
| `summary_metrics` | итоговые стоимости, flows, доход, TWR/PnL, plan и quality totals |
| `timeseries_daily` | один EOD point на имеющуюся snapshot date; cashflows относятся к `(previous_snapshot_date, current_snapshot_date]` |
| `positions_current` | alias `positions_month_end` для текущего report |
| `positions_month_start`, `positions_month_end` | нормализованные позиции boundary snapshots |
| `position_flow_groups` | `new`, `closed`, `increased`, `decreased` по quantity delta |
| `instrument_eod_timeseries` | EOD series и extrema/drawdown/rise по logical asset |
| `instrument_movers` | top growth/drawdown derived from EOD expected yield |
| `realized_by_asset`, `income_by_asset`, `open_pl_end` | attribution facts |
| `operations_top` | bounded normalized operation facts |
| `income_events`, `income_by_currency` | доходные события и currency-safe totals |
| `operation_cashflows_by_currency` | внешние flows/fees/tax facts без implicit FX |
| `reconciliation_by_asset_type` | сверка sum positions с snapshot aggregates |
| `data_quality` | explicit warnings/coverage indicators |
| `rebalance_snapshot` | target/current allocation и прочие группы |

Полный набор обязательных полей, nullability и форматов определён JSON Schema.
Дополнительные поля разрешены для additive evolution и должны игнорироваться
старыми consumers. Отсутствие любого текущего required field — нарушение v1.

## Ключевые инварианты

1. `meta.period_start` и `period_end` принадлежат одному
   `period_year/period_month`; `period_start ≤ period_end`.
2. `source_snapshot_end_id` и хотя бы один daily snapshot существуют, иначе
   payload не создаётся.
3. `positions_current` эквивалентен `positions_month_end` в текущей v1.
4. `net_external_flow = deposits - withdrawals` для base currency.
5. `total_income_net = income_net + iis_tax_deduction_income`.
6. `period_pnl_abs = end_value - start_value - net_external_flow`, когда все
   operands доступны. `period_twr_pct` — отдельно рассчитанная time-weighted
   return и не заменяется этой формулой.
7. `day_pnl` использует previous snapshot boundary и flows интервала
   `(previous_snapshot_date, current_snapshot_date]`; первый point не
   приписывает history до начала ряда.
8. `operation_cashflows_by_currency` никогда не сворачивается между валютами.
   Не-base currencies перечислены в
   `data_quality.unsupported_operation_currencies`.
9. `UNKNOWN` currency включает warning; это не эквивалент base currency.
10. AI narrative MAY отсутствовать или использовать deterministic fallback;
    `meta.has_ai_narrative` сообщает фактический source и не меняет цифры.

## Failure semantics

Builder fail-closed, если account нельзя определить, отсутствует end/daily
snapshot, период вне `1900..2100`/`1..12`, обязательная relation/query сломана
или nominal income требует неявного сложения валют. Частичный payload не
возвращается. На HTTP-границе request/data error преобразуется в documented
`400`, render/pipeline failure — в `500`.

Optional data (`rebalance_targets`, aliases, AI narrative) не делает весь
payload недоступным: отсутствие отражается пустыми rows/quality flags или
deterministic fallback. Это не разрешает скрывать missing required relations.

## Совместимость

В `v1` допустимы новые необязательные поля/sections; consumer MUST игнорировать
unknown fields. Удаление required field, изменение decimal-string/date format,
nullability, единицы, знака cashflow, interval boundary, currency aggregation,
identity semantics или значения `schema_version` требует новой major-версии.
См. [общую политику](versioning.md).

## Проверка

- Builder: `src/financetracker/reporting/report_payload.py` и `payload_*.py`.
- Queries: `src/financetracker/reporting/repository.py`.
- Consumers: `report_html.py`, `report_render.py`, `payload_ai_input.py`,
  `report_pipeline.py`.
- Tests: `tests/test_report_payload.py`,
  `tests/test_monthly_report_payload_helpers.py`,
  `tests/test_reporting_metrics.py`, `tests/test_report_render.py`,
  `tests/test_report_pipeline.py`.

```bash
python -m json.tool docs/contracts/monthly-report-payload.schema.json >/dev/null
python -m unittest tests.test_report_payload \
  tests.test_monthly_report_payload_helpers tests.test_reporting_metrics \
  tests.test_report_render tests.test_report_pipeline
```
