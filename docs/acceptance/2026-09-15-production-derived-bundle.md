# Production-derived acceptance: 2026-09-15

Статус: выполнено для рабочего checkout; не является разрешением на production
deploy и не заменяет CI опубликованного commit SHA.

## Входные данные и изоляция

- Bundle: `20260914T212835Z` вне репозитория, SHA-256 всех файлов проверены до
  restore.
- Источник bundle: production commit
  `4168a2c36926bfc8bad07813b764dc39a6ee334d`, PostgreSQL 16.13.
- Проверка выполнялась на `homeserver_external` в отдельном Compose project
  `ftbundle20260914`, отдельном volume и internal network.
- Telegram, T-Invest и Ollama не имели сетевого пути; использовались test
  credentials и deterministic tests.
- Временные database, volume, network, images и staging-каталог удалены после
  проверки.

## Candidate

Проверялся незакоммиченный рабочий checkout после рефакторинга. Идентификаторы
временных образов:

- tracker: `sha256:d98c4149b6b48e51811f37720c060bd86dbc5b9a60bf7e5d5a84490518cc5b29`;
- bot: `sha256:a03e46e2abaa101eeff62d5d020991624519302bf68b09e0bfb53552bf3b2da8`;
- reporter: `sha256:0f1a5c92c0e484dbdee7b104700140f474b8c5c6fa204e02eaa5a8497424e6e9`.

## Результаты

- Restore прошёл с `--no-owner --no-privileges`; production roles не создавались.
- Применены 16 migrations; повторный migrate применил `0`, затем
  `migrate --check` завершился успешно.
- До и после candidate migrations совпали normalised financial fingerprint,
  numeric fingerprint snapshots/positions и Decimal-normalised reconciliation.
- Canonical monthly payload за полный август 2026 совпал, кроме volatile
  `meta.generated_at_utc`.
- Dataset совпал после нормализации UTC instant, Decimal и времени генерации;
  расхождений финансовых расчётов не осталось.
- Rebalance output baseline/candidate совпал по SHA-256.
- Reporter HTTP API в изолированной сети: `/healthz=200`, неизвестный путь
  `404`, отсутствующий ключ `401`, неверный ключ `403`, invalid JSON `400`.
- PostgreSQL integration suite прошёл на чистой temporary database.

## Обнаруженные и устранённые риски

- AppleDouble файлы `._*.sql` исключены из discovery migrations и Docker context.
- Legacy blank currency в payout calendar нормализуется в `UNKNOWN`.
- Schema manifest приведён к поддерживаемому legacy type `current_nkd NUMERIC`;
  migration больше не сужает его precision.

## Ограничения

Receipt не подтверждает GitHub CI, branch protection, production deployment или
live внешние интеграции. Эти действия требуют опубликованного commit SHA и
отдельного разрешения владельца.
