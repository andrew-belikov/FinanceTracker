# Contributing

Краткие правила для изменений в `FinanceTracker`.

## Принципы
- Делайте атомарные PR: **1 PR = 1 цель**.
- Приоритет: корректность → простота → минимум изменений.
- Избегайте лишних абстракций и зависимостей.

## Локальная проверка
Из корня проекта:

```bash
python3 -m compileall src
python3 -m coverage run -m unittest discover -s tests -p 'test_*.py'
python3 -m coverage report
python3 -m ruff check src tests --select E9,F
python3 -m bandit -r src -ll -q
python3 -m mypy
python3 scripts/check_module_size.py
export APP_ENV_FILE=.env.example
docker compose --env-file "$APP_ENV_FILE" config --quiet
python3 scripts/verify_compose_env.py
python3 scripts/scan_secrets.py --history
```

GitHub Actions workflow `CI` дублирует базовую проверку на `push` и `pull_request`:
- создаёт временный `.env` из `.env.example`, проверяет его путь, владельца и права;
- устанавливает только hash-locked зависимости из `requirements/*.txt`;
- выполняет `compileall`, Ruff-проверку `E9,F`, Bandit-проверку medium-and-higher,
  строгую mypy-проверку публичных модулей config/domain/retry-policy,
  guardrail размера legacy multi-purpose modules,
  полный `unittest` suite и coverage gate production-пакета `financetracker`
  (минимум 64%);
- запускает workflow/dependency/Compose/smoke contract checks и secret scan;
- передаёт `APP_ENV_FILE` каждому вызову Compose через явный `--env-file`;
- проверяет подстановку обязательных `POSTGRES_*` на синтетическом env-файле без
  публикации значений или развёрнутого Compose-конфига.

Отдельные CI jobs также выполняют PostgreSQL 16 integration-путь (чистая
миграция, повторный запуск и `migrate --check`) и собирают все first-party
образы. Compose smoke запускает изолированные `db`/`migrate` через
`compose.ci.yml`; production external volume и сети при этом не используются.

Workflow `Deploy FinanceTracker` запускается только после успешного `CI` для
`push` в `main`. Он берёт `head_sha` завершившегося CI, создаёт одноразовый чистый
detached checkout этого SHA, проверяет точный `HEAD` и пустой `git status` до
build. Ручной и независимый от CI deploy отключён.

Образы получают tag `git-<полный SHA>`. Перед запуском deploy фиксирует их Docker
image IDs, передаёт Compose именно эти неизменяемые IDs и после `up` сверяет с
ними поле `.Image` каждого контейнера, включая одноразовую миграцию. Image IDs
вместе с CI run ID и source SHA сохраняются в summary deploy-run.
Секретный `.env` остаётся в каноническом серверном каталоге и передаётся Compose
через `APP_ENV_FILE` и явный `--env-file`. Он исключён из Docker build context
allowlist-правил `.dockerignore` и не выводится в логи.

Deploy запускает одноразовый сервис `migrate`, ждёт healthcheck сервисов и
отдельно проверяет, что pending-миграций нет. Если включён `OLLAMA_ENABLED`,
оператор передаёт также `-f compose.ollama.yml`; базовый stack не требует
внешней Ollama-сети. Уже применённые файлы миграций не редактируйте:
создавайте новый датированный forward-файл.

Checksum миграции считается по точным байтам файла. Четыре ранние миграции,
которые production ledger зарегистрировал с CRLF, закреплены в `.gitattributes`;
не нормализуйте их окончания строк. Контракт
`tests.test_migration_byte_identity` проверяет checkout policy и SHA-256 этих
уже применённых файлов.

Healthcheck `tracker` становится успешным только после начальной инициализации БД,
снимка и синхронизации операций. Healthcheck `bot` требует успешный startup smoke
и живой процесс polling. Оба сервиса поддерживают атомарный приватный ready-state,
привязанный к PID и времени старта процесса; просроченный или чужой state считается
невалидным.

## Обновление Python-зависимостей

Прямые зависимости задаются в `requirements/*.in`, а устанавливаемыми файлами
являются сгенерированные `requirements/*.txt` с exact versions и PyPI hashes.
Для обновления используйте Python 3.12 resolver и проверяйте diff lock-файлов:

```bash
uv pip compile requirements/bot.in --generate-hashes --python-version 3.12 -o requirements/bot.txt
uv pip compile requirements/dev.in --generate-hashes --python-version 3.12 -o requirements/dev.txt
uv pip compile requirements/reporter.in --generate-hashes --python-version 3.12 -o requirements/reporter.txt
uv pip compile requirements/tracker.in --generate-hashes --python-version 3.12 -o requirements/tracker.txt
```

## Версии и релизы

Используйте SemVer в формате `MAJOR.MINOR.PATCH`. Пока публичные контракты
стабилизируются, версии остаются в серии `0.x.y`: `PATCH` — исправление без
изменения контракта, `MINOR` — обратно-совместимая возможность, `MAJOR` —
несовместимое изменение. Перед релизом обновите `CHANGELOG.md`, пройдите CI и
создайте annotated tag `vMAJOR.MINOR.PATCH` для проверенного commit SHA. Не
создавайте тег или GitHub Release для непроверенного commit.

## Что обязательно в PR-описании
Используйте структуру:

### What changed
- Краткий список изменений по пунктам.

### How to verify
- Точные команды, которые запускались для проверки.

### Risks/Open questions
- Ограничения, компромиссы, неочевидные риски.

### How to apply
- Команды для обновления/запуска/логов (Docker):

```bash
export APP_ENV_FILE=.env
docker compose --env-file "$APP_ENV_FILE" up -d --build --force-recreate --remove-orphans --wait
docker compose --env-file "$APP_ENV_FILE" run --rm migrate --check
docker compose --env-file "$APP_ENV_FILE" ps
docker compose --env-file "$APP_ENV_FILE" logs --tail=200 bot
docker compose --env-file "$APP_ENV_FILE" logs --tail=200 tracker
```
