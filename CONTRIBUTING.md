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
python3 -m unittest discover -s tests -p 'test_*.py'
export APP_ENV_FILE=.env.example
docker compose --env-file "$APP_ENV_FILE" config --quiet
python3 scripts/verify_compose_env.py
python3 scripts/scan_secrets.py --history
```

GitHub Actions workflow `CI` дублирует базовую проверку на `push` и `pull_request`:
- создаёт временный `.env` из `.env.example`, проверяет его путь, владельца и права;
- устанавливает только hash-locked зависимости из `requirements/*.txt`;
- выполняет `compileall` и полный `unittest` suite;
- запускает workflow/dependency/Compose/smoke contract checks и secret scan;
- передаёт `APP_ENV_FILE` каждому вызову Compose через явный `--env-file`;
- проверяет подстановку обязательных `POSTGRES_*` на синтетическом env-файле без
  публикации значений или развёрнутого Compose-конфига.

Workflow `Deploy FinanceTracker` запускается только после успешного `CI` для
`push` в `main`. Он берёт `head_sha` завершившегося CI, создаёт одноразовый чистый
detached checkout этого SHA, проверяет точный `HEAD` и пустой `git status` до
build. Ручной и независимый от CI deploy отключён.

Образы получают tag `git-<полный SHA>`. Перед запуском deploy фиксирует их Docker
image IDs, передаёт Compose именно эти неизменяемые IDs и после `up` сверяет с
ними поле `.Image` каждого контейнера, включая одноразовую миграцию. Image IDs
вместе с CI run ID и source SHA сохраняются в summary deploy-run.
Секретный `.env` остаётся в каноническом серверном каталоге и передаётся Compose
через `APP_ENV_FILE` и явный `--env-file`; он не копируется в build context и не
выводится в логи.

Deploy подготавливает опциональную сеть `localllm_localllm`, запускает
одноразовый сервис `migrate`, ждёт healthcheck сервисов и отдельно проверяет,
что pending-миграций нет. Уже применённые файлы миграций не редактируйте:
создавайте новый датированный forward-файл.

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
uv pip compile requirements/reporter.in --generate-hashes --python-version 3.12 -o requirements/reporter.txt
uv pip compile requirements/tracker.in --generate-hashes --python-version 3.12 -o requirements/tracker.txt
```

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
