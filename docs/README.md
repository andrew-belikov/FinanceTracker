# Документация FinanceTracker

Этот индекс содержит только действующую документацию. История изменений хранится
в Git и `CHANGELOG.md`, архитектурные решения — в ADR.

## Система и контракты

- [PROJECT_MAP.md](PROJECT_MAP.md) — карта репозитория, точки входа и владельцы
  модулей.
- [ARCHITECTURE.md](ARCHITECTURE.md) — сервисы, границы и потоки данных.
- [contracts/README.md](contracts/README.md) — каталог внешних и внутренних
  контрактов, схем и способов их проверки.
- [BEHAVIOR.md](BEHAVIOR.md) — пользовательское и runtime-поведение `tracker` и
  `bot`.
- [PDF_REPORT.md](PDF_REPORT.md) — состав месячного PDF.
- [LOGGING_STANDARD.md](LOGGING_STANDARD.md) — формат, обязательные поля и
  sanitization логов.

## Эксплуатация

- [CONFIG.md](CONFIG.md) — переменные окружения и правила конфигурации.
- [RUNBOOK.md](RUNBOOK.md) — запуск, обновление, backup, restore и диагностика.

## Инженерные решения и правила

- [adr/README.md](adr/README.md) — принятые архитектурные решения.
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — разработка, проверки и выпуск.
