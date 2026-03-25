# Стандартизированное логирование

Этот документ фиксирует целевой стандарт structured logging, который можно использовать как общий контракт для любых сервисов и репозиториев.

Связанные артефакты:
- машиночитаемая схема: `logging.schema.json`
- реализация в конкретном проекте может находиться в любом shared logging module

## Цель

Нужен единый, предсказуемый и машинно-валидируемый формат runtime-логов, чтобы:
- логи всех сервисов можно было безопасно агрегировать в один поток;
- first-party события можно было фильтровать и алертить по `event`, а не по свободному тексту;
- сторонние library logs не ломали поток и были явно отличимы от curated project events;
- дочерние процессы и вспомогательные скрипты не выбрасывали plain-text мимо общего контракта;
- чувствительные данные редактировались автоматически до попадания в stdout.

## Scope

Стандарт обязателен для:
- основных runtime-сервисов;
- entrypoint-скриптов;
- healthcheck-скриптов;
- startup smoke / diagnostic helpers;
- maintenance scripts, если они пишут operational logs.

Стандарт не применяется к intentional data output, если stdout используется как полезный машинный результат, а не как лог.

Примеры типичных исключений:
- генератор конфигурации, который печатает JSON или YAML в stdout как артефакт;
- export-утилита, у которой stdout является полезным результатом для пайплайна.

## Канал вывода

- Формат: `JSON Lines`
- Одна логическая запись = один JSON-объект = одна строка в `stdout`
- `stderr` не используется для first-party логирования
- Если дочерний процесс пишет в `stdout` или `stderr`, родитель обязан перехватить поток и переизлучить его как JSON-события

## Обязательная схема

Каждая first-party runtime-запись должна содержать:
- `ts`: timestamp в ISO 8601 с timezone
- `level`: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`
- `service`: имя сервиса
- `env`: имя окружения
- `logger`: имя логгера
- `event`: стабильный идентификатор события
- `msg`: краткое человекочитаемое описание

Опциональные поля верхнего уровня:
- `ctx`: структурированный контекст события
- `trace_id`
- `request_id`
- `job_id`
- `update_id`
- `error`

Точная JSON Schema находится в файле `logging.schema.json`.

## Семантика полей

### `event`

Правила:
- только `snake_case`
- только латиница, цифры и `_`
- имя должно быть стабильным во времени
- имя должно описывать тип события, а не значение переменной
- текст `msg` может меняться, `event` не должен превращаться в prose

Хорошо:
- `snapshot_job_started`
- `daily_job_completed`
- `message_delivery_succeeded`
- `upstream_request_failed`

Плохо:
- `Daily job started`
- `send message to telegram`
- `error`
- `portfolio 2026-03-25`

### `msg`

Правила:
- короткое операторское описание происходящего
- может быть на естественном языке
- не должно дублировать весь `ctx`
- не должно использоваться как primary key для аналитики

### `ctx`

`ctx` используется для event-specific данных:
- идентификаторы сущностей
- параметры операции
- размеры выборки
- route / upstream / статус ответа
- диагностические поля

`ctx` не должен:
- дублировать верхнеуровневые поля схемы без причины
- хранить токены, пароли, секреты в открытом виде
- содержать многомегабайтные payload без усечения

### `error`

Для записей с исключением используется объект `error`:
- `type`
- `message`
- `stack`
- `where`

Если форматтер сам сломался, допускается fallback-событие `logging_formatter_failed` с минимальным `error.message`.

## Типы событий

### First-party explicit events

Целевой режим для project-owned кода:
- код явно задаёт `event`
- код явно задаёт `msg`
- дополнительный контекст передаётся в `ctx`

Это единственный допустимый режим для бизнес-событий и operational событий проекта.

### Fallback events

Если запись пришла в formatter без явного `event`, создаётся:
- `event="auto_log"`

Дополнительно в `ctx` проставляется:
- `event_source="library"` для сторонних библиотек
- `event_source="auto"` для first-party логгера, если код не задал `event` явно

Назначение fallback:
- не ломать поток логов
- не терять записи stdlib / third-party logging
- явно маркировать некурируемые события

Важно:
- `auto_log` не считается полноценным first-party контрактом
- появление `auto_log` от project-owned логгеров должно рассматриваться как технический долг или регрессия

## Child-process bridge

Если сервис запускает дочерний процесс, его stdout/stderr нельзя просто пробрасывать в контейнерный лог.

Требование:
- родительский процесс запускает child с `stdout=PIPE` и `stderr=PIPE`
- читает оба потока построчно
- на каждую строку переизлучает отдельную JSON-запись через общий logger
- в `ctx.stream` проставляет `stdout` или `stderr`

Рекомендуемый event для stream relay:
- `child_process_output`

Если bridge ломается, родитель обязан записать отдельное событие ошибки, а не молча терять поток.

## Sanitization

До записи в stdout логгер обязан редактировать:
- `token`
- `password`
- `secret`
- `api_key`
- `access_token`
- `refresh_token`
- `authorization`
- service-specific API tokens в URL и в standalone-виде
- Bearer tokens в строках

Редакция выполняется рекурсивно для строк, списков, словарей и fallback-stringification объектов.

## Correlation rules

Если в runtime доступен correlation identifier, он должен подниматься в верхний уровень записи, а не закапываться в `ctx`.

Разрешённые стандартные поля:
- `trace_id`
- `request_id`
- `job_id`
- `update_id`

Если проекту нужен новый общий correlation field, его надо сначала добавить в стандарт и в JSON Schema.

## Правила для новых проектов

При адаптации другого проекта под этот стандарт нужно обеспечить:

1. Единый shared logging module
- один модуль настройки логирования на проект
- один formatter
- единый helper / adapter для first-party кода

2. Явный first-party API
- методы вида `info(event, msg, ctx=None, **correlation)`
- запрет на implicit `(event, msg)` поверх stdlib logger

3. Покрытие всех runtime entrypoints
- основной сервис
- cron / scheduler
- worker
- healthcheck
- startup scripts
- child-process wrappers

4. Защиту от утечек
- sanitization до сериализации в JSON
- усечение слишком длинных payload

5. Guardrails
- тесты на JSON schema compliance
- тесты на redaction
- проверки против `print`
- проверки против прямого `sys.stderr.write`
- проверки против сырого child stdout/stderr

## План внедрения в существующий проект

### Этап 1. Инвентаризация

Нужно найти:
- все entrypoints
- все `print`
- все `sys.stderr.write` / `sys.stdout.write`
- все вызовы stdlib logger без `event`
- все subprocess без stream relay

### Этап 2. Общий слой

Нужно внедрить:
- shared formatter
- shared adapter
- sanitization
- schema validation tests

### Этап 3. First-party migration

Нужно перевести:
- доменные сервисы
- scheduled jobs
- handlers / controllers
- maintenance scripts

При переводе:
- давать явные `event`
- держать `msg` коротким
- переносить переменные в `ctx`

### Этап 4. Вспомогательные процессы

Нужно довести до стандарта:
- startup smoke scripts
- healthchecks
- proxy wrappers
- child processes

### Этап 5. Guardrails и rollout

Нужно добавить:
- unit tests на formatter
- regression tests на runtime scripts
- поиск forbidden patterns в executable modules
- проверку хвоста контейнерных логов на non-JSON строки

## Критерии готовности

Проект считается приведённым к стандарту, если:
- все first-party runtime logs соответствуют JSON Schema;
- все project-owned события имеют явный `event` в `snake_case`;
- library logs маркируются как `auto_log` с `ctx.event_source="library"`;
- у first-party кода нет новых `auto_log`, кроме явно согласованных исключений;
- дочерние процессы не выбрасывают сырой stdout/stderr в общий поток;
- документация фиксирует контракт, исключения и правила rollout;
- есть автоматические тесты и guardrails на регрессии.

## Примеры

### First-party success event

```json
{
  "ts": "2026-03-25T18:22:41.123456+00:00",
  "level": "INFO",
  "service": "notification_service",
  "env": "dev",
  "logger": "notifications.sender",
  "event": "message_delivery_succeeded",
  "msg": "Outbound message delivered.",
  "ctx": {
    "destination_id": 365469,
    "channel": "primary"
  },
  "update_id": 123456789
}
```

### Library fallback event

```json
{
  "ts": "2026-03-25T18:23:01.001122+00:00",
  "level": "INFO",
  "service": "notification_service",
  "env": "dev",
  "logger": "thirdparty.runtime",
  "event": "auto_log",
  "msg": "Runtime started",
  "ctx": {
    "event_source": "library"
  }
}
```

### Child-process bridged line

```json
{
  "ts": "2026-03-25T18:23:08.551122+00:00",
  "level": "INFO",
  "service": "proxy_service",
  "env": "dev",
  "logger": "child_wrapper.entrypoint",
  "event": "child_process_output",
  "msg": "child process started",
  "ctx": {
    "stream": "stderr"
  }
}
```

## Как использовать этот документ как ТЗ

Для нового или legacy-проекта достаточно трактовать этот документ как acceptance criteria:
- реализовать shared JSON logger;
- привести runtime entrypoints к explicit event/msg/ctx;
- промаркировать fallback `auto_log`;
- закрыть child-process logging;
- покрыть всё тестами и guardrails;
- подтвердить на живом запуске, что в контейнерных логах нет non-JSON строк.

## Применение в конкретном репозитории

Если стандарт используется внутри конкретного проекта, рядом с ним рекомендуется держать:
- ссылку на локальную reference implementation;
- список явных исключений из стандарта;
- проектные примеры событий;
- проверки rollout и эксплуатационные команды.
