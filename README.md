# FinanceTracker

Трекер портфеля (T‑Invest / T‑Bank Invest API) + Telegram‑бот с отчётами и уведомлениями.

## Возможности

- Сохраняет снапшоты портфеля в Postgres с заданным интервалом.
- Telegram‑бот:
  - ежедневное уведомление в заданное время;
  - недельный отчёт по пятницам;
  - месячный отчёт в последний день месяца;
  - уведомление о новом максимуме портфеля «по итогу дня».
- Ограничение доступа по списку доверенных Telegram user_id.

## Архитектура

- `tracker` — сервис, который опрашивает Invest API и пишет снапшоты в БД.
- `bot` — Telegram‑бот, который читает данные из БД и отправляет отчёты.
- `db` — Postgres.

```
Postgres  <--- tracker (snapshots)
   ^
   |
  bot (reports)
```

## Быстрый старт (Windows, Docker Desktop)

1) Клонируйте репозиторий и перейдите в папку проекта.

2) Создайте файл окружения:

```powershell
Copy-Item .\.env.example .\.env
notepad .\.env
```

Заполните минимум:
- `TELEGRAM_BOT_TOKEN`
- `TINVEST_API_TOKEN`
- `ALLOWED_USER_IDS`
- `POSTGRES_PASSWORD`

3) (Опционально) создайте внешний volume для Postgres.

Проект по умолчанию использует внешний volume `financetracker_fintracker-db` (удобно, чтобы данные переживали пересборки).

```powershell
docker volume create financetracker_fintracker-db
```

4) Запустите стек:

```powershell
docker compose up -d --build
```

Проверка статуса и логов:

```powershell
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```

## Обновление (пересборка без потери данных)

```powershell
docker compose up -d --build --force-recreate --remove-orphans
```

## Данные и бэкап

Данные Postgres хранятся в Docker volume `financetracker_fintracker-db`. Не выполняйте `docker compose down -v`, если не хотите удалить БД.

Пример дампа:

```powershell
docker exec -i financetracker-db-1 pg_dump -U $env:POSTGRES_USER $env:POSTGRES_DB > backup.sql
```


## Подготовка к AI coding

- Базовые правила для AI-агентов: `AGENTS.md`.
- Требования к PR и проверкам: `CONTRIBUTING.md`.

Перед коммитом рекомендуется выполнить:

```bash
python -m compileall src
docker compose config
```

## Конфигурация

Список переменных окружения — в `.env.example` и в `docs/CONFIG.md`.

Ежедневный JobQueue в боте теперь также делает health-check данных:
- проверяет актуальность `portfolio_snapshots` и предупреждает при отставании больше 1 дня;
- выполняет sanity-check по `deposits` (как временному источнику по операциям) и сообщает, если таблица пустая.

## Безопасность

- Никогда не коммитьте `.env`.
- Если токены/пароли где‑то засветились — перевыпустите токены и смените пароли.

## Лицензия

MIT — см. `LICENSE`.

### Проверка JobQueue после запуска (опционально)

Чтобы один раз проверить, что JobQueue реально отправляет сообщения, включите в `.env`:

```env
JOBQUEUE_SMOKE_TEST_ON_START=true
JOBQUEUE_SMOKE_TEST_DELAY_SECONDS=20
```

После старта контейнера `bot` в логе появятся события планирования и результата smoke-test.
