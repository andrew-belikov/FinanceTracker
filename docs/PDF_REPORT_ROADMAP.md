# PDF Report Roadmap

Документ фиксирует roadmap внедрения monthly PDF-отчёта до рабочего пользовательского сценария:

- ручная Telegram-команда `/monthpdf`;
- опциональная month-end автоотправка;
- narrative-layer через локальную `Ollama`, который не ломает отчёт при сбоях.

Связанные документы:

- [PDF_REPORT.md](./PDF_REPORT.md)
- [PDF_REPORT_TECH.md](./PDF_REPORT_TECH.md)

## 1. Конечная цель

После выполнения всего roadmap в проекте должно быть:

- рабочая команда `/monthpdf`, которая отправляет PDF-отчёт за текущий месяц;
- отдельный deterministic pipeline `payload -> charts -> HTML -> PDF`;
- отдельный AI-layer `Ollama`, который улучшает narrative, но не влияет на расчёты;
- безопасный deployment на `homeserver` через сеть `localllm_localllm`;
- month-end auto-send, использующий тот же pipeline;
- понятные env-переменные, docs и smoke-проверки.

## 2. Архитектурное решение для roadmap

Roadmap ниже исходит из одного базового решения:

- не встраивать PDF-рендер и `Ollama` прямо в polling runtime бота;
- добавить отдельный внутренний `reporter` service;
- подключить `reporter` к двум сетям:
  - `financetracker_default` для доступа к `db`;
  - `localllm_localllm` для доступа к `Ollama`;
- оставить `bot` тонким клиентом, который:
  - принимает `/monthpdf`;
  - вызывает `reporter` по внутреннему HTTP;
  - получает готовый PDF;
  - отправляет документ в Telegram.

Почему это целевой путь:

- не утяжеляет `docker/Dockerfile.bot`;
- не смешивает WeasyPrint/Ollama runtime с Telegram polling;
- соответствует фактической сетевой схеме на `homeserver`;
- уменьшает риск регрессий в основном боте.

## 3. Продуктовые решения, которые считаются зафиксированными

- Основная ручная команда: `/monthpdf`.
- Текущая команда `/month` остаётся текстовой и быстрой.
- Первый PDF-формат: только `monthly review`.
- Первая рабочая версия команды может быть без `Ollama`.
- Автоотправка в month-end включается только после того, как ручная команда уже стабильна.
- Для month-end auto-send primary format должен быть один:
  - либо PDF;
  - либо текст.

Для этого roadmap рекомендован такой порядок:

- до финального PR auto-send продолжает слать текст;
- в финальном PR month-end переключается на PDF как primary monthly artifact.

## 4. Agent Playbook

### 4.1 Общая стратегия по агентам

Для ускорения и снижения риска лучше использовать агентов не “по числу задач”, а по типу работы.

Рекомендуемая схема:

- Lead integrator:
  - модель: `gpt-5.4`
  - reasoning: `high`
  - роль: держит архитектурную линию, принимает финальные решения, сводит PR.
- Fast explorers:
  - модель: `gpt-5.4-mini`
  - reasoning: `low`
  - роль: быстро размечают поверхности файлов, entrypoints, env, тесты.
- Contract explorers:
  - модель: `gpt-5.4-mini`
  - reasoning: `medium`
  - роль: выискивают edge cases, shape данных, backward-compatibility риски.
- Focused workers:
  - модель: `gpt-5.4-mini`
  - reasoning: `medium` или `high`
  - роль: делают bounded implementation на непересекающихся файлах.

### 4.2 Когда какой depth использовать

- `low`
  Использовать для:
  - inventory файлов;
  - поиска точек интеграции;
  - проверки регистраций команд, env и compose.

- `medium`
  Использовать для:
  - новых data contracts;
  - handler/job wiring;
  - тестов на shape данных;
  - rollout docs и config.

- `high`
  Использовать для:
  - склейки pipeline;
  - AI validation/fallback;
  - deployment/runtime решений;
  - финального review перед merge.

### 4.3 Правило на каждый PR

Для каждого PR:

1. До начала:
   - `1-2` explorer на `gpt-5.4-mini`.
2. Во время реализации:
   - `1-2` worker на `gpt-5.4-mini` с непересекающимися write scopes.
3. Перед финальной склейкой:
   - основной агент `gpt-5.4` с `high`.

## 5. Roadmap по PR

Ниже roadmap разбит на `6` PR.

### PR 1. `feat(reporter): добавить reporter service и внутренний RPC-контур`

Рекомендуемая ветка:

- `feat/pdf-reporter-infra`

Цель:

- подготовить безопасный runtime-каркас для PDF without touching polling path.

Что должно появиться:

- новый `reporter` service в `compose.yml`;
- новый `docker/Dockerfile.reporter`;
- новый `requirements/reporter.txt`;
- внутренний health endpoint `GET /healthz`;
- внутренний endpoint `POST /reports/monthly/pdf`;
- подключение `reporter` к сети `localllm_localllm`;
- env-конфиг для `OLLAMA_BASE_URL=http://ollama:11434`;
- базовый internal client contract между `bot` и `reporter`.

Файлы:

- [compose.yml](/Users/andrew/Dev/FinanceTracker/compose.yml)
- `docker/Dockerfile.reporter`
- `requirements/reporter.txt`
- `src/bot/report_pipeline.py`
- `src/bot/report_server.py` или аналогичный entrypoint
- [docs/CONFIG.md](/Users/andrew/Dev/FinanceTracker/docs/CONFIG.md)
- [README.md](/Users/andrew/Dev/FinanceTracker/README.md)

Решения этого PR:

- `reporter` не публикуется наружу на host ports;
- `reporter` доступен только внутри Docker-сетей;
- `bot` пока не использует endpoint в пользовательском сценарии;
- `Ollama` пока может быть отключена, но сетевой маршрут уже должен быть корректным.

Критерий готовности:

- `docker compose up -d reporter` поднимает сервис;
- `reporter` healthy;
- из контейнера `reporter` доступен `http://ollama:11434/api/tags`;
- `docker compose config` описывает внешнюю сеть корректно;
- `bot` продолжает работать без изменений поведения.

Проверка:

- `python3 -m compileall src`
- `docker compose config`
- `docker compose build reporter`
- `docker compose up -d reporter`
- `docker compose exec -T reporter python - <<'PY' ...`
  Проверить `http://ollama:11434/api/tags`
- `docker compose logs --tail=100 reporter`

Агенты:

- explorer `gpt-5.4-mini`, `low`
  Проверить `compose.yml`, сети, env и healthcheck surface.
- worker `gpt-5.4-mini`, `medium`
  Ownership: `compose.yml`, `docker/`, `requirements/`.
- lead `gpt-5.4`, `high`
  Финальная склейка сетевой и runtime-части.

Риски:

- ошибка во внешней сети `localllm_localllm`;
- утечка лишних портов наружу;
- случайное утяжеление `bot` вместо `reporter`.

Вне scope:

- реальный PDF;
- Telegram-команда;
- AI narrative.

### PR 2. `feat(report-data): собрать deterministic monthly_report_payload`

Рекомендуемая ветка:

- `feat/pdf-monthly-payload`

Цель:

- получить канонический structured payload для monthly PDF.

Что должно появиться:

- `monthly_report_payload.v1`;
- `monthly_ai_input.v1`;
- query для `instrument_eod_timeseries`;
- builder для `position_flow_groups` с value delta;
- builder для `operations_top`;
- serialization rules для `Decimal/date/datetime`;
- debug path для сохранения payload при необходимости.

Файлы:

- `src/bot/report_payload.py`
- [src/bot/queries.py](/Users/andrew/Dev/FinanceTracker/src/bot/queries.py)
- [src/bot/services.py](/Users/andrew/Dev/FinanceTracker/src/bot/services.py)
- [tests/test_month_positions_diff.py](/Users/andrew/Dev/FinanceTracker/tests/test_month_positions_diff.py)
- новый `tests/test_report_payload.py`
- [docs/PDF_REPORT_TECH.md](/Users/andrew/Dev/FinanceTracker/docs/PDF_REPORT_TECH.md)

Критерий готовности:

- payload строится без LLM;
- shape данных стабилен и сериализуем;
- `instrument_eod_timeseries` честно работает на EOD-only данных;
- пустые `income_events`, `targets`, `alias`-дыры не ломают payload.

Проверка:

- `python3 -m unittest discover -s tests -p "test_*.py"`
- targeted tests:
  - `tests/test_report_payload.py`
  - `tests/test_month_positions_diff.py`
  - `tests/test_dataset_helpers.py`
  - `tests/test_reporting_metrics.py`
- ручной debug dump payload на реальных данных.

Агенты:

- explorer `gpt-5.4-mini`, `medium`
  Ownership: query inventory, edge cases по `logical_asset_id`.
- worker `gpt-5.4-mini`, `high`
  Ownership: `queries.py` + tests.
- worker `gpt-5.4-mini`, `medium`
  Ownership: `report_payload.py` + serialization helpers + tests.
- lead `gpt-5.4`, `high`
  Проверка, что payload реально соответствует техдоке.

Риски:

- склейка инструментов на best-effort alias;
- неверная трактовка `open_pl_end` как monthly contribution;
- поломка shape данных без явных runtime ошибок.

Вне scope:

- HTML/PDF render;
- Telegram delivery;
- AI calls.

### PR 3. `feat(report-render): deterministic HTML и PDF без AI`

Рекомендуемая ветка:

- `feat/pdf-render-core`

Цель:

- научить `reporter` собирать monthly PDF полностью без `Ollama`.

Что должно появиться:

- HTML template для 5-страничного отчёта;
- chart artifact builders для monthly PDF;
- deterministic fallback narrative builder;
- PDF renderer в `reporter`;
- endpoint `POST /reports/monthly/pdf` возвращает готовый PDF.

Файлы:

- `src/bot/report_render.py`
- `src/bot/report_templates/monthly_report.html.j2` или аналогичный путь
- [src/bot/charts.py](/Users/andrew/Dev/FinanceTracker/src/bot/charts.py)
- `src/bot/report_pipeline.py`
- новый `tests/test_report_render.py`
- [docs/PDF_REPORT.md](/Users/andrew/Dev/FinanceTracker/docs/PDF_REPORT.md)

Критерий готовности:

- по запросу в `reporter` получается валидный PDF;
- отчёт собирается без `Ollama`;
- пустые блоки корректно скрываются;
- page breaks стабильно работают на `A4 portrait`.

Проверка:

- `python3 -m unittest discover -s tests -p "test_*.py"`
- HTTP smoke:
  - `curl` в `reporter` endpoint;
- файл PDF открывается;
- минимум один ручной smoke на реальных данных `homeserver`.

Агенты:

- explorer `gpt-5.4-mini`, `low`
  Ownership: template/render integration points.
- worker `gpt-5.4-mini`, `medium`
  Ownership: HTML template + CSS.
- worker `gpt-5.4-mini`, `high`
  Ownership: PDF render path + endpoint response contract.
- lead `gpt-5.4`, `high`
  Финальная вычитка layout и fallback behavior.

Риски:

- системные зависимости `WeasyPrint`;
- нестабильные page breaks;
- чрезмерная логика в шаблоне.

Вне scope:

- ручная Telegram-команда;
- `Ollama`.

### PR 4. `feat(bot): ручная команда /monthpdf и отправка документа`

Рекомендуемая ветка:

- `feat/monthpdf-command`

Цель:

- довести систему до первого пользовательского milestone: working `/monthpdf`.

Что должно появиться:

- команда `/monthpdf` в Telegram;
- `safe_send_document()` в runtime layer;
- bot-side internal client для вызова `reporter`;
- короткий ack пользователю перед долгой генерацией;
- отправка готового PDF в текущий чат.

Файлы:

- [src/bot/handlers.py](/Users/andrew/Dev/FinanceTracker/src/bot/handlers.py)
- [src/bot/bot.py](/Users/andrew/Dev/FinanceTracker/src/bot/bot.py)
- [src/bot/runtime.py](/Users/andrew/Dev/FinanceTracker/src/bot/runtime.py)
- [src/bot/services.py](/Users/andrew/Dev/FinanceTracker/src/bot/services.py)
- новый `src/bot/report_client.py`
- новый `tests/test_monthpdf_command.py`
- [docs/CONFIG.md](/Users/andrew/Dev/FinanceTracker/docs/CONFIG.md)

Командный контракт MVP:

- `/monthpdf`
  Отправляет PDF за текущий месяц по тем же периодным правилам, что и `/month`.

Опционально, если не сильно удорожает реализацию:

- `/monthpdf YYYY-MM`

Но это не должно тормозить MVP.

Критерий готовности:

- пользователь пишет `/monthpdf`;
- получает быстрый ack;
- затем получает document в чат;
- `/month` и прочие команды не деградируют.

Проверка:

- `python3 -m unittest discover -s tests -p "test_*.py"`
- targeted tests:
  - `tests/test_monthpdf_command.py`
  - `tests/test_runtime_logging_guardrails.py`
  - `tests/test_bot_schedule_config.py`
- ручной smoke в Telegram на `homeserver`.

Агенты:

- explorer `gpt-5.4-mini`, `low`
  Ownership: handler registration, help text, UX path.
- worker `gpt-5.4-mini`, `medium`
  Ownership: `handlers.py`, `bot.py`, `services.py`.
- worker `gpt-5.4-mini`, `medium`
  Ownership: `runtime.py`, `report_client.py`, tests.
- lead `gpt-5.4`, `high`
  Финальный command UX и delivery semantics.

Риски:

- задержка генерации и таймауты;
- проблемы upload документа в Telegram;
- лишняя блокировка event loop, если internal client сделан неаккуратно.

Вне scope:

- month-end auto-send;
- AI narrative.

Майлстоун:

- по завершении PR 4 система уже удовлетворяет базовой цели “есть рабочая команда PDF-отчёта”.

### PR 5. `feat(report-ai): narrative generation через Ollama с жестким fallback`

Рекомендуемая ветка:

- `feat/pdf-ollama-narrative`

Цель:

- подключить `Ollama` как чистый narrative-layer, не нарушая deterministic report core.

Что должно появиться:

- `monthly_ai_input.v1`;
- `monthly_ai_output.v1`;
- client к `POST /api/chat`;
- JSON schema validation;
- semantic validation на “не придумывать новые числа”;
- один repair-attempt;
- fallback на deterministic narrative.

Файлы:

- `src/bot/report_ai.py`
- `src/bot/report_pipeline.py`
- новый `tests/test_report_ai.py`
- [docs/PDF_REPORT_TECH.md](/Users/andrew/Dev/FinanceTracker/docs/PDF_REPORT_TECH.md)
- [docs/CONFIG.md](/Users/andrew/Dev/FinanceTracker/docs/CONFIG.md)

Критерий готовности:

- PDF собирается и с AI, и без AI;
- при timeout/invalid JSON/reporter error выдаётся fallback;
- новые числа из LLM не проходят в итоговый PDF.

Проверка:

- unit tests на schema и semantic validation;
- smoke без `OLLAMA_ENABLED`;
- smoke с реальной `Ollama` на `homeserver`;
- проверка на последовательные, а не параллельные запросы.

Агенты:

- explorer `gpt-5.4-mini`, `medium`
  Ownership: prompt/schema/fallback contract.
- worker `gpt-5.4-mini`, `high`
  Ownership: `report_ai.py`, validators, repair flow.
- worker `gpt-5.4-mini`, `medium`
  Ownership: env/docs/tests.
- lead `gpt-5.4`, `high`
  Проверка AI safety boundaries и fallback semantics.

Риски:

- слабый narrative на `qwen2.5:1.5b`;
- schema-valid, но semantic-invalid ответ;
- очередь запросов при `OLLAMA_NUM_PARALLEL=1`.

Вне scope:

- автоотправка;
- идемпотентность month-end deliveries.

### PR 6. `feat(report-delivery): month-end auto-send и operational hardening`

Рекомендуемая ветка:

- `feat/monthpdf-autosend`

Цель:

- сделать эксплуатационный, а не только ручной сценарий monthly PDF.

Что должно появиться:

- вызов report pipeline из `daily_job` при `is_month_end`;
- month-level idempotency для PDF-доставки;
- решение по primary monthly artifact:
  - в этом roadmap рекомендовано переключить month-end на PDF;
- timeout/retry policy;
- логирование этапов генерации и доставки;
- rollout docs и how-to-apply.

Файлы:

- [src/bot/jobs.py](/Users/andrew/Dev/FinanceTracker/src/bot/jobs.py)
- [src/bot/runtime.py](/Users/andrew/Dev/FinanceTracker/src/bot/runtime.py)
- `src/bot/report_client.py`
- новые `tests/test_monthpdf_delivery.py`
- [README.md](/Users/andrew/Dev/FinanceTracker/README.md)
- [docs/CONFIG.md](/Users/andrew/Dev/FinanceTracker/docs/CONFIG.md)

Критерий готовности:

- в последний день месяца daily job вызывает PDF pipeline;
- PDF не дублируется при ручном и автоматическом сценарии;
- сбой AI или рендера не валит весь daily job;
- rollout и эксплуатация описаны в docs.

Проверка:

- `python3 -m unittest discover -s tests -p "test_*.py"`
- targeted tests:
  - `tests/test_bot_daily_job_catchup.py`
  - `tests/test_monthpdf_delivery.py`
  - `tests/test_runtime_logging_guardrails.py`
- `docker compose config`
- end-to-end smoke на `homeserver` в controlled manual run.

Агенты:

- explorer `gpt-5.4-mini`, `medium`
  Ownership: daily job behavior и idempotency points.
- worker `gpt-5.4-mini`, `high`
  Ownership: `jobs.py`, delivery policy, tests.
- worker `gpt-5.4-mini`, `medium`
  Ownership: docs/config/how-to-apply.
- lead `gpt-5.4`, `high`
  Финальный operational review.

Риски:

- дубли доставки;
- шум в чатах;
- зависания month-end job из-за внешних зависимостей.

Вне scope:

- yearly PDF;
- web UI;
- richer AI commentary.

## 6. Что именно считается “готово”

Минимально приемлемая точка “у нас уже есть рабочая команда PDF-отчёта”:

- завершён `PR 4`.

Полностью production-ready точка:

- завершён `PR 6`.

Разница:

- после `PR 4` команда `/monthpdf` уже работает вручную;
- после `PR 6` отчёт встроен в эксплуатационный контур и устойчив к реальным сбоям.

## 7. Базовая проверка на каждом PR

Минимум:

- `python3 -m compileall src`
- `python3 -m unittest discover -s tests -p "test_*.py"`
- `docker compose config`

Дополнительно по roadmap:

- targeted smoke для соответствующего слоя;
- если PR затрагивает deployment, обязательный `docker compose build`;
- если PR затрагивает `reporter`, smoke на `homeserver`.

## 8. На что не нужно тратить PR раньше времени

До `PR 4` не стоит:

- делать yearly PDF;
- вводить новый web UI;
- усложнять prompt engineering ради красоты текста;
- добавлять вложенные сценарии команд;
- делать сложные sparkline на инструментальной странице.

До `PR 5` не стоит:

- делать AI обязательным для отчёта;
- тюнить модель раньше, чем готов deterministic PDF.

## 9. Рекомендуемый порядок мержа

Порядок важен:

1. `PR 1` — инфраструктурный каркас.
2. `PR 2` — данные и контракты.
3. `PR 3` — deterministic render.
4. `PR 4` — ручная пользовательская команда.
5. `PR 5` — AI-layer.
6. `PR 6` — автоотправка и hardening.

Если хочется ускорить получение первой пользы:

- можно мержить `PR 1-4` как обязательный контур;
- `PR 5-6` считать второй волной.

## 10. Короткий итог

Самый безопасный и практичный путь здесь такой:

- вынести PDF в отдельный `reporter`;
- сначала сделать deterministic PDF;
- затем довести до ручной команды `/monthpdf`;
- только после этого подключить `Ollama`;
- и уже потом включить month-end auto-send и hardening.

Так мы быстрее получаем рабочую команду, не ломаем polling-бота и не делаем AI критической зависимостью для отчёта.
