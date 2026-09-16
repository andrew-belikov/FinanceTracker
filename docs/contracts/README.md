# Контракты FinanceTracker

Этот каталог — каноническая точка входа для межкомпонентных и долговечных
контрактов. Он описывает только фактически реализованное поведение. Исходный
код, миграции и автоматические проверки остаются исполняемыми источниками
истины; расхождение документа с ними считается дефектом документации.

## Реестр

| Контракт | Версия | Статус | Владелец | Машиночитаемая спецификация |
|---|---:|---|---|---|
| [Схема PostgreSQL](database-schema.md) | `schema_manifest.v2` | active | `financetracker.database` | `src/financetracker/database/schema_manifest.py` |
| [Reporter HTTP API](reporter-http-api.md) | `monthly_report_service.v1` | active, internal | `financetracker.reporting` | [OpenAPI 3.1](reporter.openapi.json) |
| [Monthly report payload](monthly-report-payload.md) | `monthly_report_payload.v1` | active, internal | `financetracker.reporting` | [JSON Schema](monthly-report-payload.schema.json) |
| [Конфигурация runtime](configuration.md) | `runtime_configuration.v1` | active | владельцы сервисов | `.env.example`, `compose*.yml`, `src/financetracker/**/runtime.py` |
| [Внешние интеграции](external-integrations.md) | `external_integrations.v1` | active | владельцы consumers | upstream API + contract tests |
| [Совместимость и версионирование](versioning.md) | `contract_policy.v1` | active | maintainers | — |

Связанные действующие контракты вне этого каталога:

- [пользовательское поведение и формулы](../BEHAVIOR.md);
- [структурированное логирование](../LOGGING_STANDARD.md) и его
  [JSON Schema](../logging.schema.json);
- [состав месячного PDF](../PDF_REPORT.md).

## Область действия

Контракт обязателен, если значение пересекает границу процесса, контейнера,
хранилища или независимо изменяемого модуля. Внутренние Python-функции без
такой границы не становятся публичным API только из-за упоминания здесь.

Нормативные слова **MUST**, **MUST NOT**, **SHOULD** и **MAY** следует понимать
как требования, запреты, рекомендации и разрешения соответственно.

## Порядок изменения

1. Определить producer, consumers и класс совместимости по
   [политике версий](versioning.md).
2. Обновить код, машиночитаемую спецификацию, этот реестр и проверки в одном
   изменении.
3. Для несовместимого изменения создать новую major-версию и переходный период;
   существующую версию не переопределять задним числом.
4. Для DB сначала добавить forward-only migration, затем обновить manifest.
5. Не использовать production payload, идентификаторы, токены и финансовые
   данные в примерах или fixtures.

## Проверка

Из корня репозитория:

```bash
python -m json.tool docs/contracts/reporter.openapi.json >/dev/null
python -m json.tool docs/contracts/monthly-report-payload.schema.json >/dev/null
python -m unittest tests.test_markdown_links tests.test_tracker_migrations \
  tests.test_report_server tests.test_report_payload
```

HTTP-тесты, открывающие loopback socket, могут быть недоступны в ограниченной
песочнице. Это ограничение среды необходимо отделять от дефекта приложения.
