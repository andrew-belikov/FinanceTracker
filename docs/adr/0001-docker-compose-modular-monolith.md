# ADR-0001: Docker Compose и модульный монолит

Статус: принято

## Решение

FinanceTracker остаётся одним репозиторием и одним Docker Compose-проектом.
Runtime-роли `tracker`, `bot`, `reporter`, `migrate` и `xray-client` собираются
из одного installable package `financetracker`.

## Причины

Роли имеют разные side effects и lifecycle, но используют одну финансовую
модель и PostgreSQL system of record. Отдельные сервисы или репозитории добавили
бы сетевые и data-consistency контракты без оправданной продуктовой потребности.

## Последствия

Границы модулей и импортов обязательны; физическое разделение применяется к
runtime-ролям, а не к каждой domain-области. Compose service names и DB volume
остаются совместимыми контрактами.
