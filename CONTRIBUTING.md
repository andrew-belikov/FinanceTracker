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
docker compose config > /dev/null
```

GitHub Actions workflow `CI` дублирует базовую проверку на `push` и `pull_request`:
- создаёт временный `.env` из `.env.example`;
- выполняет `compileall`;
- запускает `unittest`;
- проверяет `docker compose config` без публикации развёрнутого вывода с секретами.

Workflow `Deploy FinanceTracker` автоматически запускается на `push` в `main` на self-hosted runner и работает с каноническим каталогом сервера `/home/andrey/projects/FinanceTracker`, а не с runner workspace.
Ручной `workflow_dispatch` оставлен как fallback: в режиме `smoke` можно проверить runner и серверный checkout без перезапуска контейнеров, в режиме `deploy` — повторить обычный deploy вручную.

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
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```
