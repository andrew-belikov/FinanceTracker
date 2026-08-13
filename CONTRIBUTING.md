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
docker compose config --quiet
python3 scripts/secret_scan.py --history
```

GitHub Actions workflow `CI` дублирует базовую проверку на `push` и `pull_request`:
- создаёт временный `.env` из `.env.example`;
- устанавливает только hash-locked зависимости из `requirements/*.txt`;
- выполняет `compileall` и полный `unittest` suite;
- запускает workflow/dependency/Compose/smoke contract checks и secret scan;
- проверяет `docker compose config --quiet` без публикации развёрнутого вывода с секретами.

Workflow `Deploy FinanceTracker` запускается только после успешного `CI` для
`push` в `main`. Он берёт `head_sha` завершившегося CI, создаёт одноразовый чистый
detached checkout этого SHA, проверяет точный `HEAD` и пустой `git status` до
build. Ручной и независимый от CI deploy отключён.

Образы получают tag `git-<полный SHA>`, запускаются с `--no-build`, а их Docker
image IDs вместе с CI run ID и source SHA сохраняются в summary deploy-run.
Секретный `.env` остаётся в каноническом серверном каталоге и передаётся Compose
через `APP_ENV_FILE`; он не копируется в build context.

Deploy подготавливает опциональную сеть `localllm_localllm`, запускает
одноразовый сервис `migrate`, ждёт healthcheck сервисов и отдельно проверяет,
что pending-миграций нет. Уже применённые файлы миграций не редактируйте:
создавайте новый датированный forward-файл.

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
docker compose up -d --build --force-recreate --remove-orphans --wait
docker compose run --rm migrate --check
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```
