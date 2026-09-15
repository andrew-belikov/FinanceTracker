# ADR-0002: PostgreSQL и forward-only migrations

Статус: принято

## Решение

PostgreSQL — единственный system of record. Схема изменяется versioned SQL
migrations с checksum ledger; ORM не создаёт production-схему. `migrate --check`
проверяет migrations и декларативный schema manifest.

## Причины

Runtime использует PostgreSQL-специфичный SQL и требует воспроизводимого
upgrade-path. Автоматическое создание ORM-схемы маскирует drift и не доказывает
историю изменений.

## Последствия

Миграции forward-only. Rollback приложения допускается только совместимый со
схемой; восстановление данных требует backup/restore процедуры из runbook.
