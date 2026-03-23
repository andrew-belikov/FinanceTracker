# FinanceTracker

`FinanceTracker` сохраняет историю портфеля из T-Invest / T-Bank Invest API в Postgres и отдаёт отчёты через Telegram-бота.

В составе проекта:
- `tracker` опрашивает Invest API, пишет снапшоты портфеля и синхронизирует операции.
- `bot` читает данные из БД, отвечает на команды Telegram и рассылает автоматические уведомления.
- `db` хранит снапшоты, позиции, операции и события доходов.

## Что умеет проект

- Сохраняет снапшоты портфеля в Postgres по расписанию `interval` или `cron`.
- Синхронизирует операции счёта в таблицу `operations`.
- Строит отчёты и графики по командам `/today`, `/week`, `/month`, `/year`, `/structure`, `/history`, `/twr`.
- Отправляет автоматические сообщения: недельный отчёт, месячный отчёт, новый максимум портфеля, выполнение годового плана пополнений, зачисление купонов и дивидендов.
- Ограничивает доступ к боту по `ALLOWED_USER_IDS`.

## Карта документации

- [Конфигурация](docs/CONFIG.md)
- [Архитектура](docs/ARCHITECTURE.md)
- [Поведение команд и фоновых задач](docs/BEHAVIOR.md)
- [Runbook: запуск, обновление, миграции, проверки](docs/RUNBOOK.md)
- [Правила для контрибьюторов](CONTRIBUTING.md)
- [Инструкции для AI-агентов](AGENTS.md)

## Быстрый старт

Основной compose-файл находится в корне репозитория: [`compose.yml`](compose.yml). Файл [`docker/compose.yml`](docker/compose.yml) оставлен только для совместимости.

1. Скопируйте пример окружения:

```powershell
Copy-Item .\.env.example .\.env
notepad .\.env
```

2. Заполните минимум:

- `POSTGRES_PASSWORD`
- `TELEGRAM_BOT_TOKEN`
- `TINVEST_API_TOKEN`
- `ALLOWED_USER_IDS`

3. Создайте внешний volume для Postgres:

```powershell
docker volume create financetracker_fintracker-db
```

4. Поднимите стек:

```powershell
docker compose up -d --build
```

5. Проверьте статус и логи:

```powershell
docker compose ps
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

## Модель времени

В проекте используются три разных времени, и это важно для интерпретации отчётов:

- `SCHED_TZ` управляет планировщиком `tracker` и тем, какая дата попадёт в `portfolio_snapshots.snapshot_date`.
- `TIMEZONE` используется ботом для отображения дат и для части date-based расчётов.
- Ежедневный JobQueue у бота запускается в `18:00` по локальному времени контейнера, а не по `TIMEZONE`.

`TIMEZONE=Europe/Moscow` не переводит автоматические рассылки бота на московское время. Если нужен фиксированный civil time для авторассылок, настраивайте локальную таймзону контейнера отдельно. Подробности и последствия описаны в [docs/CONFIG.md](docs/CONFIG.md) и [docs/BEHAVIOR.md](docs/BEHAVIOR.md).

## Clean Install И Upgrade

На пустой БД проект запускается без ручных SQL-миграций: `tracker` вызывает `Base.metadata.create_all()` и создаёт таблицы, достаточные для работы текущего кода.

Важно:

- чистая ORM-схема не полностью совпадает со схемой после всех SQL-миграций;
- на чистой БД `deposits` будет обычной таблицей, а не compatibility-view;
- глобальная уникальность `operations.operation_id` появляется только после миграции `migrations/20260304_operations_operation_item_fields.sql`.

Если у вас уже есть существующая БД или нужна точная migrated-схема с compatibility-объектами, действуйте через [docs/RUNBOOK.md](docs/RUNBOOK.md): сделайте backup, остановите `tracker` и `bot`, примените нужные SQL-миграции и только потом поднимайте сервисы обратно.

## Базовые Docker-команды

Запуск:

```bash
docker compose up -d --build
```

Обновление без потери volume:

```bash
docker compose up -d --build --force-recreate --remove-orphans
```

Остановка:

```bash
docker compose stop
```

Просмотр логов:

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Не используйте `docker compose down -v`, если хотите сохранить данные Postgres.

## Короткий Smoke-Check После Деплоя

1. Убедитесь, что контейнеры запущены:

```bash
docker compose ps
```

2. Убедитесь, что в логах нет ошибок подключения к БД, Telegram или Invest API:

```bash
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

3. Проверьте в Telegram:

- `/today`
- `/week`
- `/month`
- `/year`
- `/history`
- `/twr`

4. Если нужны SQL-проверки, готовые запросы есть в [docs/RUNBOOK.md](docs/RUNBOOK.md).

## Проверка Репозитория

Из корня проекта:

```bash
python3 -m unittest discover -s tests
```

Дополнительно полезно:

- `python -m compileall src` указан в `AGENTS.md`, но alias `python` может отсутствовать в среде;
- если `python3 -m compileall src` упирается в системный cache path, используйте `PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m compileall src`;
- `docker compose config` требует существующий локальный `.env`, потому что `compose.yml` использует `env_file: .env`.

## Безопасность

- Не коммитьте `.env`.
- Если токены или пароли попали в логи, историю shell или внешний канал, перевыпустите токены и смените пароли.
