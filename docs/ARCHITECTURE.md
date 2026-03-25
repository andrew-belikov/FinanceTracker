# Архитектура

## Сервисы

- `db` (Postgres) — хранит снапшоты и вспомогательные данные.
- `tracker` — периодически опрашивает Invest API и пишет снапшоты портфеля в БД.
- `bot` — Telegram‑бот, читает данные из БД, формирует отчёты и уведомления.

## Поток данных

1) `tracker` получает текущую стоимость портфеля и дополнительные данные.
2) Записывает снапшот в Postgres.
3) `bot` по расписанию читает данные и отправляет сообщения в Telegram.

## Логирование

- `tracker`, `bot` и `xray-client` используют единый JSON logger из `src/common/logging_setup.py`.
- Startup helpers (`bot/entrypoint.py`, `bot/proxy_smoke.py`), healthcheck `xray-client` и maintenance scripts тоже пишут structured JSON logs.
- Каждая first-party runtime-запись содержит как минимум `ts`, `level`, `service`, `env`, `logger`, `event`, `msg`; дополнительный контекст идёт в `ctx`.
- Подробная спецификация и ТЗ для тиражирования стандарта находятся в [LOGGING_STANDARD.md](/Users/andrew/Dev/FinanceTracker/docs/LOGGING_STANDARD.md), а машиночитаемая схема — в [logging.schema.json](/Users/andrew/Dev/FinanceTracker/docs/logging.schema.json).
- Для first-party кода целевой контракт такой: `event` задаётся явно, остаётся стабильным и использует `snake_case`.
- Записи без явного `event` получают `event="auto_log"` и `ctx.event_source`; это fallback для stdlib/library logging, а не основной путь для project-owned кода.
- Значение `ctx.event_source="library"` используется для сторонних библиотек, `ctx.event_source="auto"` — для auto-tagging first-party записи.
- Для `xray-client` stdout/stderr дочернего процесса `xray` перехватываются и переизлучаются как события `xray_process_output`, поэтому контейнерный log stream остаётся JSON-only.
- Для bridged child-process логов имя потока передаётся в `ctx.stream`.
- Исключение: `xray_client/render_config.py` — это intentional data output, он печатает конфиг в stdout и не считается логированием.

## Примечания

- Данные Postgres живут в Docker volume (по умолчанию внешний `financetracker_fintracker-db`).
