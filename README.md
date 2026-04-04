# FinanceTracker

Трекер портфеля (T‑Invest / T‑Bank Invest API) + Telegram‑бот с отчётами и уведомлениями.

## Возможности

- Сохраняет снапшоты портфеля в Postgres (режим interval или cron, в зависимости от `SNAPSHOT_MODE`).
- Telegram‑бот:
  - недельный отчёт по пятницам в заданное время JobQueue (по умолчанию `18:00` по таймзоне `TIMEZONE`);
  - в дневных/недельных/месячных отчётах показываются доходы (купоны, дивиденды) и расходы (комиссии, налоги);
  - изменение стоимости в `/today`, `/week` и `/month` считается без внешних пополнений и выводов; выводы из расчёта исключаются, но отдельной строкой не показываются;
  - месячный отчёт в последний день месяца в заданное время JobQueue (по умолчанию `18:00` по таймзоне `TIMEZONE`);
  - команда `/targets` показывает текущие таргеты аллокации, а `/targets set stocks=50 bonds=30 cash=20` сохраняет целевые доли по классам активов;
  - команда `/rebalance` показывает текущие отклонения от таргетов и расчёт buy/sell по классам, чтобы вернуться к целевой структуре;
  - команда `/invest <sum>` подсказывает, как распределить новое пополнение по таргетам;
  - команда `/twr` с corrected дневным period-first TWR по активному счёту, XIRR в годовых, run-rate на 31 декабря (без новых внешних cashflow) и графиком по дням в двух связанных панелях: стоимость портфеля и TWR;
  - команда `/dataset` отправляет один ZIP-архив для AI-анализа: внутри `dataset.json`, `daily_timeseries.csv`, `positions_current.csv`, `operations.csv`, `income_events.csv` и `README_AI.md`; summary в архиве period-first, а не lifetime-first, плюс есть `logical_asset_id`, `reconciliation_*` и quality flags;
  - команда `/year` работает в двух режимах: без аргумента — отчёт за текущий год (YTD), с аргументом `/year YYYY` — отчёт за указанный календарный год;
  - `/year` отправляет 4 сообщения: summary, PNG-график с двумя связанными блоками (сверху — стоимость портфеля на конец месяца, снизу — пополнения за месяц), дополнительный PNG-график `результат по месяцам` (дельта без учёта пополнений: изменение стоимости минус пополнения за период месяца), и movements (сгруппированный блок изменений позиций 🆕/📈/📉/✅ по первому и последнему снапшоту внутри периода, с агрегацией по логическому активу: ticker → name → figi; валютные позиции из qty-движений исключаются);
  - для `/year` финансовые суммы считаются только по исполненным операциям (`state = OPERATION_STATE_EXECUTED`) c дедупликацией по `operation_id` (последняя запись по `id`), чтобы не было задвоений;
  - в `/year` добавлены блоки top-N по активам (с строкой `Итого`) и ключевые формулы: realized net по продажам = `yield + commission`, income net по купонам/дивидендам = `gross + tax`, unrealized на конец периода берётся из последнего снапшота периода;
  - авто‑уведомления о зачислении купонов и дивидендов (по событиям из БД);
  - авто‑подсказка по каждому новому пополнению счёта: бот автоматически отправляет рекомендацию в логике `/invest <сумма>`;
  - уведомление о новом максимуме портфеля «по итогу дня»;
  - уведомление о выполнении годового плана пополнений.
- Ограничение доступа по списку доверенных Telegram user_id.
- `tracker` при старте контейнера автоматически устанавливает дополнительные доверенные сертификаты из `docker/certs/`, поэтому deploy с пересозданием контейнера обновляет trust store без ручных действий.

## Гайдлайн по графикам

- Один график отвечает на один главный вопрос; если вопросов два, лучше разделить на две связанные панели.
- Главная серия должна быть самой контрастной, вторичные серии спокойнее и тоньше.
- Вместо перегруженной легенды предпочтительны прямые подписи на последних значениях и на ключевых экстремумах.
- Подписи и оси должны использовать короткий человекочитаемый формат: `янв`, `фев`, `1.2 млн ₽`, `+4 %`.
- Положительная динамика всегда показывается одним семейством цветов, отрицательная — другим; нейтральные состояния не должны спорить с акцентами.
- Сетка остаётся только вспомогательной: мягкая по оси Y, без лишнего шума по оси X.
- Если данные разных масштабов мешают чтению, их нужно разводить по отдельным панелям, а не уплотнять в одну ось.
- График в Telegram должен быть понятен без зума: крупные акценты, минимум декоративных деталей, не больше 2-3 основных цветов.

## Архитектура

- `tracker` — сервис, который опрашивает Invest API и пишет снапшоты в БД.
- `bot` — Telegram‑бот, который читает данные из БД, хранит таргеты аллокации и отправляет отчёты.
- `db` — Postgres.

```
Postgres  <--- tracker (snapshots)
   ^
   |
  bot (reports)
```

## Быстрый старт (Linux/macOS, Docker)

Единственный поддерживаемый operational workflow: запуск `docker compose` из корня репозитория с корневым `compose.yml`.

1) Клонируйте репозиторий и перейдите в папку проекта.

2) Создайте файл окружения:

```bash
cp .env.example .env
${EDITOR:-vi} .env
```

Заполните минимум:
- `TELEGRAM_BOT_TOKEN`
- `TINVEST_API_TOKEN`
- `ALLOWED_USER_IDS`
- `POSTGRES_PASSWORD`

3) (Опционально) создайте внешний volume для Postgres.

Проект по умолчанию использует внешний volume `financetracker_fintracker-db`, чтобы данные переживали пересборки.

```bash
docker volume create financetracker_fintracker-db
```

4) Запустите стек:

```bash
docker compose up -d --build
```

5) Проверьте статус и логи:

```bash
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```

### Опциональный outbound proxy только для `bot`

Если на сервере прямой доступ к Telegram нестабилен, можно включить VLESS+Reality только для контейнера `bot`.
`tracker` и `db` при этом продолжают работать напрямую, без proxy.

Добавьте в `.env`:

```env
BOT_PROXY_ENABLED=true
BOT_VLESS_URL="vless://uuid@host:443?encryption=none&security=reality&sni=example.com&fp=chrome&pbk=PUBLIC_KEY&sid=SHORT_ID&type=tcp#bot"
BOT_VLESS_FALLBACK_URL="vless://uuid@backup-host:8443?encryption=...&security=none&type=kcp#bot-backup"
```

Чтобы выключить режим, достаточно вернуть:

```env
BOT_PROXY_ENABLED=false
BOT_VLESS_URL=""
BOT_VLESS_FALLBACK_URL=""
```

После изменения `.env` пересоберите и перезапустите стек:

```bash
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps
docker compose logs --tail=200 xray-client
docker compose logs --tail=200 bot
```

Быстрый smoke-test маршрута `bot -> xray-client -> Telegram`:

```bash
docker compose exec bot python proxy_smoke.py
```

Сценарий проверки:
- при `BOT_PROXY_ENABLED=true` `docker compose ps` показывает `xray-client` в состоянии `healthy`;
- при `BOT_PROXY_ENABLED=false` `xray-client` не попадает в обычный `docker compose ps`, потому что proxy не активен; при необходимости детальный статус виден в `docker compose ps -a xray-client` как `Exited (0)`;
- все runtime-логи `bot`, `tracker`, `xray-client`, startup smoke и healthcheck идут как JSON Lines в `stdout`; удобнее всего смотреть их через `docker compose logs ...`;
- при `BOT_PROXY_ENABLED=true` `bot` подключается к локальному SOCKS endpoint `socks5h://xray-client:1080`, а `xray-client` сам проверяет внешний маршрут через `https://api.ipify.org`;
- если задан `BOT_VLESS_FALLBACK_URL`, контейнер `xray-client` при неуспешном старте основного маршрута автоматически пробует fallback-ссылку и фиксирует активную роль в status file и логах;
- основной и fallback URL могут отличаться по transport/security; текущий код умеет как Reality/TCP, так и VLESS с `security=none` и `type=kcp`;
- в логах `xray-client` ищите события `xray_proxy_ready`, `xray_proxy_smoke_completed` и `xray_process_output`;
- `proxy_smoke.py` внутри `bot` пишет одно структурированное событие `bot_startup_smoke_completed` или `bot_startup_smoke_failed` и подтверждает доступность Telegram API и прямой TCP-доступ к `db`;
- long polling бота и обычные Bot API вызовы используют один и тот же явный proxy endpoint `BOT_PROXY_ENDPOINT`, чтобы `getUpdates` не зависел от неявного env-resolve внутри клиента;
- если Telegram API временно недоступен через proxy, `bot.py` возвращает специальный код supervision, а `entrypoint.py` перезапускает процесс бота внутри контейнера с паузой `BOT_STARTUP_RETRY_DELAY_SECONDS` вместо жёсткого crash-loop всего контейнера;
- watchdog long polling подтверждает backlog двумя подряд проверками и при подтверждённом зависании завершает процесс бота, чтобы `restart: unless-stopped` автоматически поднял контейнер заново;
- при `BOT_PROXY_ENABLED=false` `xray-client` пишет событие `xray_proxy_disabled`, а `tracker` продолжает работать как раньше.

### Правила structured logging

- Все first-party runtime-процессы проекта пишут по одной JSON-записи на строку в `stdout`.
- Базовая схема записи: `ts`, `level`, `service`, `env`, `logger`, `event`, `msg`; дополнительный контекст передаётся в `ctx`.
- Для project-owned кода `event` должен быть явным, стабильным и в `snake_case`.
- Если запись пришла без явного `event`, formatter назначает `event="auto_log"` и добавляет `ctx.event_source`.
- `ctx.event_source="library"` означает, что запись пришла от сторонней библиотеки через стандартный `logging`.
- `ctx.event_source="auto"` означает auto-tagging для first-party логгера и считается fallback-путём, а не целевым контрактом.
- Для дочерних процессов используется bridge в общий logger: строки из stdout/stderr переизлучаются как отдельные JSON-события с `ctx.stream`.
- Исключение только одно: [src/xray_client/render_config.py](/Users/andrew/Dev/FinanceTracker/src/xray_client/render_config.py) печатает конфиг в stdout как data output и не считается логированием.
- Подробная спека уровня ТЗ находится в [docs/LOGGING_STANDARD.md](/Users/andrew/Dev/FinanceTracker/docs/LOGGING_STANDARD.md), JSON Schema — в [docs/logging.schema.json](/Users/andrew/Dev/FinanceTracker/docs/logging.schema.json).

## Обновление (пересборка без потери данных)

```bash
docker compose up -d --build --force-recreate --remove-orphans
```

## Деплой с перезапуском всех сервисов без потери данных

Надёжный порядок (перезапуск всех сервисов + сохранность БД):

```bash
# 1) (Опционально, но рекомендуется) бэкап текущей БД
docker compose exec -T db pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > backup_before_redeploy.sql

# 2) Обновить код
git pull --ff-only

# 3) Пересобрать и перезапустить весь стек
# ВАЖНО: не использовать down -v (это удалит volume с данными)
docker compose up -d --build --force-recreate --remove-orphans

# 4) Проверить, что все сервисы поднялись
docker compose ps

# 5) Проверить логи приложений
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Критично для сохранности данных:
- данные Postgres живут в Docker volume `financetracker_fintracker-db`;
- команда `docker compose down` сама по себе данные не удаляет;
- **не запускать** `docker compose down -v`, если нужна сохранность БД.

### GitHub Actions deploy

- На GitHub настроен workflow `Deploy FinanceTracker`, который автоматически запускается при `push` в `main`.
- Workflow выполняется на self-hosted runner на сервере и работает только с каноническим каталогом `/home/andrey/projects/FinanceTracker`.
- Порядок workflow:
  - `git fetch origin`
  - `git checkout main`
  - `git pull --ff-only origin main`
  - `python3 -m compileall src`
  - `docker compose config > /dev/null`
  - `docker compose up -d --build --force-recreate --remove-orphans`
  - `docker compose ps`
  - `docker compose logs --tail=200 bot tracker`
- Ручной запуск через `workflow_dispatch` оставлен как fallback:
  - `mode=smoke` — безопасная проверка runner, git checkout и `docker compose ps` без перезапуска контейнеров;
  - `mode=deploy` — ручной повтор обычного deploy-сценария.


## Данные и бэкап

Данные Postgres хранятся в Docker volume `financetracker_fintracker-db`. Не выполняйте `docker compose down -v`, если не хотите удалить БД.

Пример дампа:

```bash
docker compose exec -T db pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > backup.sql
```


## Подготовка к AI coding

- Базовые правила для AI-агентов: `AGENTS.md`.
- Требования к PR и проверкам: `CONTRIBUTING.md`.

Перед коммитом рекомендуется выполнить:

```bash
python3 -m compileall src
docker compose config > /dev/null
```


## SQL-миграции

На чистой БД проект стартует без ручного применения SQL-скриптов. Исторические миграции нужны только при обновлении уже существующей базы или при переходе со старой схемы `deposits`.

Активный код читает пополнения и прочие операции из `operations`. View `deposits`, если он вообще присутствует, нужен только как исторический compatibility-артефакт старой миграции.

Основные SQL-файлы:
- `migrations/20260221_operations_from_deposits.sql` — переводит старую схему `deposits` в `operations` и оставляет исторический compatibility-view `deposits`;
- `migrations/20260225_operations_add_instrument_columns.sql` — добавляет в `operations` поля `instrument_uid` и `figi`;
- `migrations/20260226_income_events.sql` — создаёт таблицу `income_events` для уведомлений и отчётных сумм;
- `migrations/20260304_operations_operation_item_fields.sql` — расширяет `operations` полями из `OperationItem` и добавляет уникальность по `operation_id`;
- `migrations/20260324_dataset_source_fields.sql` — добавляет поля для source-aware `/dataset` и таблицу `asset_aliases`;
- `migrations/20260324_rebalance_targets_and_invest_notifications.sql` — создаёт `rebalance_targets` и `invest_notifications`.

### Историческая миграция со схемы `deposits`

```bash
# 1) Остановить writer/reader, чтобы зафиксировать состояние на время миграции
docker compose stop tracker bot

# 2) Сделать бэкап перед изменениями
docker compose exec -T db pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > pre_operations_migration_backup.sql

# 3) Применить историческую SQL-миграцию
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260221_operations_from_deposits.sql

# 4) Поднять сервисы обратно
docker compose up -d tracker bot

# 5) Проверить, что активный код работает через operations
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose logs --tail=200 tracker
docker compose logs --tail=200 bot
```

Если вы обновляли именно старую `deposits`-схему, можно дополнительно проверить исторический compatibility-view:

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d deposits"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS deposits_rows FROM deposits;"
```

Откат с ограничениями остаётся в `migrations/20260221_operations_from_deposits.rollback.sql`: он возвращает `deposits` как таблицу при наличии `deposits_legacy`, но не удаляет `operations`.

### Дополнительные миграции

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260225_operations_add_instrument_columns.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260226_income_events.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260304_operations_operation_item_fields.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260324_dataset_source_fields.sql
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < migrations/20260324_rebalance_targets_and_invest_notifications.sql
```

Нюансы:
- `tracker` после миграций в `operations` может сделать backfill пустых полей для уже существующих строк;
- `income_events` нужен для минутных уведомлений о купонах/дивидендах и части отчётных сумм;
- миграцию `20260324_rebalance_targets_and_invest_notifications.sql` нужно применить до пересборки `tracker`, иначе новый код не сможет писать расширенные поля `portfolio_positions`.

### Разовый repair после старого Windows backup

Если после старого Windows backup в `operations.description` появилась битая кириллица, можно выполнить разовый repair внутри контейнера `tracker`:

```bash
docker compose exec -T tracker python repair_operations_description_encoding.py
```

## Конфигурация

Список переменных окружения — в `.env.example` и в `docs/CONFIG.md`.

Авто-рассылки JobQueue:
- каждый день в `DAILY_SUMMARY_HOUR:DAILY_SUMMARY_MINUTE` по таймзоне `TIMEZONE` проверяются триггеры: новый максимум и выполнение годового плана;
- по пятницам в то же время дополнительно отправляется недельный отчёт;
- в последний день месяца в то же время дополнительно отправляется месячный отчёт.


## Чек-лист проверки бота после обновления

Ниже — минимальный практический smoke-check, чтобы убедиться, что обновление прошло корректно.

1) Проверить, что контейнеры запущены:

```bash
docker compose ps
```

2) Проверить логи на старте (без traceback/DB errors):

```bash
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```

3) Проверить данные операций в БД (по умолчанию бот считает пополнениями `OPERATION_TYPE_INPUT`):

```bash
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT operation_type, COUNT(*) FROM operations GROUP BY operation_type ORDER BY operation_type;"
docker compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT MAX(date)::date AS latest_input_date FROM operations WHERE operation_type='OPERATION_TYPE_INPUT';"
```

4) В Telegram вручную проверить команды бота:

- `/today` — приходит сводка; дневная дельта считается как изменение стоимости минус внешние пополнения/выводы за локальный день, а общий PnL по-прежнему считается относительно суммы пополнений.
- `/week` — динамика считается относительно последнего снапшота до начала текущей рабочей недели (понедельник–пятница) и очищается от внешних пополнений/выводов за период; суммы за период остаются отдельными строками.
- `/month` — динамика считается относительно последнего снапшота до начала месяца и очищается от внешних пополнений/выводов за месяц; также есть сумма пополнений за месяц и прогресс годового плана.
- `/year` — режим YTD (без аргумента): приходит summary за текущий год + 2 PNG-графика (основной и `прирост/падение по месяцам`) + movements (четвёртое сообщение с блоками изменений позиций по группам, агрегация в movements идёт по ticker с fallback на name/figi); график `прирост/падение по месяцам` считается без учёта пополнений (изменение стоимости минус пополнения за период месяца).
- `/year YYYY` — режим календарного года: аналогичный отчёт за указанный год (summary + 2 PNG-графика + movements).
- В summary `/year` формулы считаются так: realized net = `yield + commission`, income net = `gross + tax`, unrealized на конец периода — из последнего снапшота внутри выбранного периода.
- `/history` — строится график стоимости и пополнений (без ошибок отправки файла).
- `/structure` — корректно отрисовывается структура портфеля.
- `/twr` — приходит corrected дневной period-first TWR по активному счёту, XIRR в годовых, run-rate на 31 декабря без новых пополнений/выводов и график по дням.
- `/dataset` — приходит ZIP-архив с `json + csv + md`, пригодный для передачи ИИ-модели; внутри summary используются `period_*` поля, `has_full_history_from_zero`, `reconciliation_gap_abs`, а в `positions_current`/`operations` есть `logical_asset_id`.

## Бэклог `/dataset`

- Разложить `reconciliation_gap_abs` на именованные компоненты источника (`cash_free`, `cash_blocked`, `accrued_interest`, `other_assets_value`), если Invest API отдаёт их стабильно и однозначно.
- Довести source-aware reconciliation до полного совпадения не только по классам активов, но и по экономическому смыслу остатка внутри bond/cash-компонентов.
- Добавить честную lifetime-аналитику только после подтверждения полной истории портфеля с нуля и корректного cost basis по всем текущим позициям.
- Вынести lifetime-метрики в отдельный контракт датасета, чтобы не смешивать их с текущим period-first summary.
- Достроить strict cost basis / realized / unrealized analytics по логическому активу, а не только по текущему `figi`.
- Расширить alias-слой проверками и отчётами, чтобы смена FIGI автоматически подсвечивалась и не ломала downstream-аналитику.
- Добавить интеграционную проверку после миграции `20260324_dataset_source_fields.sql`: alias-группы заполняются, а `logical_asset_id` стабилен между операциями и текущими позициями.

5) Проверить авто-рассылки JobQueue:

- в логах `bot` есть событие `bot_started` с `ctx.daily_job_schedule` и последующие события запуска `daily_job_started`;
- по пятницам приходит `/week`;
- в последний день месяца приходит `/month`;
- при новом максимуме/пересечении годового плана приходит соответствующий триггер.

Рекомендуется после деплоя выполнить команды из этого чек-листа и сохранить короткий отчёт: время проверки, какие команды Telegram протестированы, и результат.

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
