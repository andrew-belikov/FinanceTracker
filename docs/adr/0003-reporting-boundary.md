# ADR-0003: граница reporting

Статус: принято

## Решение

`reporting` владеет read-only SQL monthly report, versioned payload, narrative и
HTML/PDF rendering. Bot запрашивает PDF только через внутренний authenticated
HTTP contract и не импортирует внутренности reporting.

## Причины

Это сохраняет один владелец report pipeline и исключает Telegram runtime из
reporter image. Передача полного financial payload из bot была отклонена: она
создала бы два владельца reporting queries.

## Последствия

Renderer получает готовый payload и не открывает DB session. Изменение PDF не
должно требовать изменения Telegram handlers.
