# Документация FinanceTracker

## Действующие источники истины

- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) — целевые границы модулей,
  сервисов и зависимостей.
- [CONTRACTS.md](CONTRACTS.md) — нормативные контракты данных, денег, времени,
  миграций, интеграций и наблюдаемости.
- [ARCHITECTURE.md](ARCHITECTURE.md) — фактическая переходная архитектура
  текущего checkout.
- [BEHAVIOR.md](BEHAVIOR.md) — подтверждённое runtime-поведение tracker и bot.
- [CONFIG.md](CONFIG.md) — переменные окружения и configuration contracts.
- [RUNBOOK.md](RUNBOOK.md) — эксплуатация, backup и восстановление.
- [LOGGING_STANDARD.md](LOGGING_STANDARD.md) — обязательная структура и
  sanitization логов.
- [PDF_REPORT.md](PDF_REPORT.md) — продуктовый контракт monthly PDF.
- [adr/README.md](adr/README.md) — принятые архитектурные решения и их
  последствия.

## План и отслеживание работ

- [PROJECT_AUDIT.md](PROJECT_AUDIT.md) — датированный audit/roadmap: это не
  источник текущего runtime-состояния и не замена runbook.
- [acceptance/2026-09-15-production-derived-bundle.md](acceptance/2026-09-15-production-derived-bundle.md)
  — обезличенный receipt проверки production-derived bundle для текущего
  refactor checkout.
- [archive/PDF_REPORT_ROADMAP.md](archive/PDF_REPORT_ROADMAP.md) — исторический
  roadmap PDF; фактические контракты ищите в `PDF_REPORT.md` и `CONTRACTS.md`.
- [archive/PDF_REPORT_TECH.md](archive/PDF_REPORT_TECH.md) — исторический
  технический контекст PDF, который не отменяет действующие contracts.
- [archive/bot-decomposition-plan.md](archive/bot-decomposition-plan.md) —
  исторический план flat-layout; текущая структура описана в `ARCHITECTURE.md`.

## Репозиторные правила и исторические материалы

- [../CONTRIBUTING.md](../CONTRIBUTING.md) — правила разработки и проверки.
- [../AGENTS.md](../AGENTS.md) — инструкции для automation agents.
- [../CODE_REVIEW.md](../CODE_REVIEW.md) — исторический audit конкретного
  commit; не описывает текущий checkout.
- [../CODE_REVIEW_REMEDIATION.md](../CODE_REVIEW_REMEDIATION.md) — журнал
  устранения findings исторического audit.
