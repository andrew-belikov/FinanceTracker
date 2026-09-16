# Контракт структурированного логирования

Этот документ фиксирует действующий runtime-контракт FinanceTracker. Он
применяется вместе с машиночитаемой схемой [`logging.schema.json`](logging.schema.json)
и реализацией `financetracker.common.logging_setup`.

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

Значения `error.message` и `error.stack` проходят тот же sanitizer, что и
остальные строки. Сам объект не редактируется целиком: безопасные `type` и
`where` обязательны для различения transport, TLS, proxy и программных сбоев.

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

Штатные строки child-process Xray (например, accepted connections) имеют
уровень `DEBUG`; строки с warning/error/failure сохраняют соответствующий
`WARNING`/`ERROR`. События failover и recovery остаются явными first-party
событиями уровня `INFO` или выше. Автоматические сообщения APScheduler
разрешены только от `WARNING`, чтобы минутные job executions не маскировали
инциденты.

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

## Реализация и проверка

- Канонический formatter, sanitizer и first-party API находятся в
  `src/financetracker/common/logging_setup.py`.
- Контракт обязателен для `tracker`, `bot`, `reporter`, `xray-client`, их
  entrypoint и healthcheck-процессов.
- `tests/test_logging_setup.py` проверяет форму событий и fallback.
- `tests/test_logging_privacy_contract.py` проверяет редакцию чувствительных
  данных.
- `tests/test_runtime_logging_guardrails.py` запрещает обход общего слоя в
  runtime entrypoints.
- JSON Schema должна изменяться атомарно с formatter и соответствующими
  тестами. Удаление или изменение обязательного поля считается несовместимым
  изменением контракта.

Внешняя проверка выполняется по JSON Lines в `docker compose logs`: каждая
first-party строка должна разбираться как JSON и соответствовать
[`logging.schema.json`](logging.schema.json). Plain-text от сторонней библиотеки
допустим только после преобразования formatter в `auto_log`.
