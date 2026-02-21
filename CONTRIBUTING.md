# Contributing

Краткие правила для изменений в `FinanceTracker`.

## Принципы
- Делайте атомарные PR: **1 PR = 1 цель**.
- Приоритет: корректность → простота → минимум изменений.
- Избегайте лишних абстракций и зависимостей.

## Локальная проверка
Из корня проекта:

```bash
python -m compileall src
docker compose config
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
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 tracker
```
