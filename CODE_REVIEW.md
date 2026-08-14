# Подробное код-ревью FinanceTracker

Дата: 2026-08-13

Проверенный commit: `fb512f8` (`main`)

Формат: статический аудит кода и конфигурации, точечные воспроизведения чистых функций, доступные локальные проверки. Код приложения не изменялся.

## Итог

Найдено 39 замечаний: 1 блокирующее (`P0`), 14 критичных для корректности/эксплуатации (`P1`), 21 существенное (`P2`) и 3 пункта укрепления (`P3`). До исправления `P0` и `P1` текущую версию нельзя считать безопасной и доказанно корректной для финансовой отчётности или автоматического production deploy.

Главные риски:

1. В tracked test fixture находятся полные строки доступа к VLESS, внешне похожие на рабочие credentials.
2. Уведомления о доходах падают до отправки; кнопка ИИС подключена к неправильному потоку.
3. Деньги и доходность считаются неверно при пропусках снапшотов, выводах, дивидендах/комиссиях, смешанных валютах и на границах локальных дат.
4. Account-scoping операций нарушен глобальной уникальностью `operation_id`; явно заданный account ID не является fail-closed.
5. Deploy может начаться при упавшем CI и собрать грязный checkout, а readiness приложений фактически не доказывается.
6. Reporter доступен без аутентификации из общей Docker-сети и может выдавать приватный отчёт либо использоваться для истощения ресурсов.

Это ревью существенно уменьшает область неизвестности, но не является доказательством отсутствия иных ошибок: полноценный suite, PostgreSQL integration, Docker runtime и реальные внешние API локально не запускались.

## План и распределение шести независимых проходов

Из-за ограничения среды на четыре одновременно активных процесса работа выполнена двумя волнами по три агента, без форка контекста. Каждый агент получил требование самостоятельно просмотреть весь tracked-код и затем углубиться в свою область.

| Проход | Основной фокус | Дополнительная сквозная проверка |
|---|---|---|
| 1 | Корректность core/API и обработка ошибок | `src`, миграции, tests, config, docs |
| 2 | Финансовая математика, TWR/XIRR, отчёты и dataset | Все пути денежных потоков и календарных границ |
| 3 | Security, privacy, reliability, Docker и supply chain | Все внешние входы, секреты, fail-open defaults |
| 4 | Миграции, schema/runtime contracts и account isolation | Все SQL/query/upsert пути |
| 5 | Tests, CI/CD, deploy и gaps покрытия | Все workflow и false-green состояния |
| 6 | Сквозные adversarial-сценарии | Падения, повторы, конкуренция, неполные данные |

После агентских проходов корневой проход повторно проверил источники, объединил дубли, исключил недоказанные гипотезы и выполнил точечные воспроизведения арифметики.

## Шкала серьёзности

- `P0` — возможная компрометация секрета; требуется немедленная реакция.
- `P1` — потеря ключевой функции, утечка приватных данных, существенная финансовая ошибка или небезопасный deploy.
- `P2` — достижимая ошибка надёжности/целостности/эксплуатации с более узким условием.
- `P3` — hardening и снижение будущего риска.

## Найденные проблемы

### P0-01 — В репозитории находятся полные строки доступа к VLESS

**Где:** `tests/test_xray_proxy_config.py:24-33`.

**Условие:** любой, кто читает repository или его историю, открывает fixture.

**Доказательство:** тест содержит две полные VLESS URL со внешними адресами, UUID и параметрами Reality. Значения намеренно не воспроизводятся в отчёте. Их фактическая работоспособность не проверялась, чтобы не выполнять несанкционированное подключение; до доказательства синтетичности их следует считать скомпрометированными.

**Последствие:** несанкционированное использование прокси и раскрытие сетевой инфраструктуры.

**Исправление:** сначала отозвать/ротировать соответствующие credentials; затем заменить fixture синтетическими значениями и RFC 5737 адресами, очистить историю после ротации и добавить secret scanning. Одного удаления из текущего commit недостаточно.

**Тест:** secret scanner не находит URI/UUID/private-key patterns; unit-тесты используют только явно синтетические строки.

### P1-01 — Income notifier падает на каждой новой выплате, а кнопка ИИС подключена не туда

**Где:** `src/bot/queries.py:2036-2085`, `src/bot/jobs.py:903-920`, `src/bot/jobs.py:960-1015`, контракт `README.md:24-25`.

**Условие:** появляется `income_events.notified=false`.

**Доказательство:** query не выбирает `operation_id` и `cashflow_category`, но `check_income_events()` обращается к `row["operation_id"]` до `try`. Получается `KeyError`, строка остаётся ненотифицированной, ошибка повторяется каждую минуту. У реальных строк пополнения нужные поля есть, но `check_invest_notifications()` не передаёт `reply_markup`.

**Последствие:** уведомления о купонах/дивидендах не отправляются; обещанная кнопка классификации ИИС отсутствует на пополнении.

**Исправление:** удалить IIS markup из income notifier и формировать/передавать его в invest notifier; зафиксировать типизированный контракт результата query.

**Тест:** async-тест обоих jobs с точной формой реальных query rows; income отправляется и помечается, deposit получает callback `iis_deduction:set:<operation_id>`.

### P1-02 — Production deploy не зависит от успешного CI

**Где:** `.github/workflows/ci.yml:3-5`, `.github/workflows/deploy.yml:3-6`, `.github/workflows/deploy.yml:67-85`.

**Условие:** push в `main` содержит регрессию.

**Доказательство:** CI и deploy — независимые workflow на одно событие. Deploy проверяет только compileall/Compose и может пересоздать production до завершения или после падения unit tests.

**Последствие:** заведомо красный commit может попасть в эксплуатацию.

**Исправление:** запускать deploy только по успешному `workflow_run` CI с тем же точным SHA либо объединить jobs через `needs`.

**Тест:** workflow-contract запрещает deploy при `conclusion != success` и при несовпадении проверенного/deploy SHA.

### P1-03 — Локальные календарные границы сравниваются с UTC-naive датами

**Где:** `src/tracker/app.py:1592-1596`, `src/tracker/app.py:1747-1755`, `src/bot/services.py:832-856`, `src/bot/services.py:906-1035`, `src/bot/queries.py:142-172`, `src/bot/queries.py:207-353`, `src/bot/report_payload.py:89-102`, `src/bot/dataset.py:61-69`.

**Условие:** операция проходит около локальной полуночи, особенно на границе месяца/года.

**Доказательство:** tracker сохраняет UTC без timezone; bot строит naive границы из локальной гражданской даты и напрямую сравнивает их с UTC-naive колонкой. Например, `00:30 MSK` первого числа хранится как `21:30 UTC` предыдущего дня и выпадает из нового месяца.

**Последствие:** неверны day/week/month/year, планы, налоги, доходы, отчёты и dataset; разные подсистемы могут группировать одну операцию в разные дни.

**Исправление:** один общий helper `local [start,end) -> UTC-naive` и явное локальное преобразование при группировке; предпочтительно мигрировать timestamps на `TIMESTAMPTZ`.

**Тест:** table-driven cases до/после полуночи на границах дня, месяца, года и DST-zone.

### P1-04 — Налог из income events имеет неправильный знак в отчётах

**Где:** `src/bot/queries.py:312-353`, `src/bot/services.py:851-856`, `src/bot/services.py:958-963`, `src/bot/services.py:1024-1029`, `src/bot/dataset.py:166-169`, `tests/test_tracker_api_resilience.py:351-375`.

**Условие:** выплата содержит удержанный налог с API-знаком `-13`, одновременно могут быть operation taxes.

**Доказательство:** SQL суммирует отрицательный `income_events.tax_amount` без нормализации и прибавляет к положительной сумме прочих налогов. Dataset уже использует `abs`, то есть два продукта расходятся.

**Последствие:** строка налогов занижена или отрицательна.

**Исправление:** определить канонический signed-cashflow contract; удержания нормализовать как положительный расход, возвраты налогов моделировать отдельно.

**Тест:** income tax `-13` и operation tax `-5` должны дать расход `18`, плюс отдельный кейс tax refund.

### P1-05 — TWR и `/today` неверны при пропусках снапшотов

**Где:** `src/bot/services.py:462-491`, `src/bot/services.py:832-881`, `src/bot/services.py:2642-2666`.

**Условие:** между двумя соседними имеющимися снапшотами есть день без снапшота и внешний поток.

**Доказательство:** `compute_twr_series()` вычитает поток только на дате правого снапшота. `/today` берёт два последних снапшота, но вычитает только поток текущего дня. При `100` на 01.08, пополнении `50` 02.08 и `150` на 03.08 получается ложный `+50%` вместо `0%`.

**Последствие:** TWR, daily delta и основанные на них выводы завышаются/занижаются.

**Исправление:** суммировать внешний поток на всём полуинтервале между соседними снапшотами; `/today` использовать тот же interval contract.

**Тест:** gap-day deposit/withdrawal, несколько потоков, нулевой предыдущий value.

### P1-06 — `period_twr_pct` месячного PDF и dataset на самом деле lifetime TWR

**Где:** `src/bot/report_payload.py:1333-1341`, `src/bot/report_payload.py:1425-1426`, `src/bot/report_render.py:594-598`, `src/bot/dataset.py:324`.

**Условие:** до отчётного месяца уже была доходность.

**Доказательство:** код фильтрует накопительный lifetime TWR series по датам месяца и берёт последнее значение, не rebasing на начало периода. При `+20%` до месяца и нулевой доходности внутри месяца отчёт показывает `+20%` как результат месяца. Текущий тест `tests/test_report_payload.py:467-473,525` закрепляет это ошибочное значение.

**Последствие:** ключевой KPI периода неверен в PDF и AI dataset.

**Исправление:** rebasing накопительного множителя по точке перед `period_start` или отдельный расчёт period TWR.

**Тест:** ненулевая pre-period доходность и плоский месяц должны дать `0%`.

### P1-07 — Daily/fallback P&L ошибочно исключает доходы и возвращает комиссии/налоги

**Где:** `src/bot/report_payload.py:403-453`, `src/bot/report_payload.py:480-499`, `src/bot/dataset.py:187-223`, для сравнения `src/bot/dataset.py:279-287`.

**Условие:** день содержит дивиденд/купон, комиссию или налог; либо в периоде нет стартового снапшота.

**Доказательство:** `day_pnl = delta - (deposit - withdrawal + income - commission - tax)`. Доход вычитается из результата, а комиссия/налог прибавляются обратно. В fallback period P&L используется та же смесь, тогда как summary path вычитает только внешний поток.

**Последствие:** dividend-only и fee-only дни отображаются как нулевой P&L; значение периода зависит от наличия стартового снапшота.

**Исправление:** из изменения стоимости вычитать только внешние вклады/выводы; доходы, комиссии и налоги оставлять компонентами portfolio result. Налоговый вычет ИИС остаётся portfolio income, но не external flow.

**Тест:** изолированные dividend, commission, tax, deposit, withdrawal и отсутствие start snapshot.

### P1-08 — Lifetime P&L `/today` считает вывод средств убытком

**Где:** `src/bot/queries.py:115-139`, `src/bot/services.py:850`, `src/bot/services.py:883-887`.

**Условие:** пользователь выводил средства.

**Доказательство:** lifetime P&L равен `current_value - total_deposits`; withdrawals не добавляются обратно. После вклада `100`, вывода `50` и остатка `50` без рыночного движения показывается `-50` вместо `0`.

**Последствие:** абсолютный и процентный результат портфеля систематически занижен после любого вывода.

**Исправление:** использовать net external contribution `deposits - withdrawals`, сохранив отдельный contract для IIS deduction.

**Тест:** deposit/withdrawal round-trip и несколько выводов в разные даты.

### P1-09 — Денежные агрегаты смешивают валюты без FX-конвертации

**Где:** `src/tracker/app.py:276`, `src/tracker/app.py:286-308`, `migrations/20260226_income_events.sql:1-14`, `src/tracker/app.py:1735-1755`, `src/bot/queries.py:263-353`, `src/bot/queries.py:1043-1104`, `src/bot/queries.py:1157-1224`, `src/bot/report_payload.py:383-398`, `src/bot/dataset.py:492-505`.

**Условие:** account содержит выплаты/налоги не только в базовой валюте.

**Доказательство:** `Operation` хранит currency, но `IncomeEvent` её не хранит; reconcile группирует только по FIGI/date/type, а отчёты суммируют номиналы и подписывают результат как базовую валюту.

**Последствие:** например, USD и RUB складываются как числа; финансовые итоги становятся бессмысленными.

**Исправление:** добавить currency в event identity и все агрегаты; либо раздельно показывать валюты, либо конвертировать по документированному FX source/date; при неизвестном курсе fail closed.

**Тест:** смешанные RUB/USD выплаты и налоги, отсутствие FX rate.

### P1-10 — Глобальный `operation_id` ломает account-scoped модель

**Где:** `src/tracker/app.py:239-247`, `migrations/20260221_operations_from_deposits.sql:17-27`, `migrations/20260304_operations_operation_item_fields.sql:24-34`, `src/tracker/app.py:1643-1650`.

**Условие:** два счёта получают одинаковый broker `operation_id` или меняется выбранный account.

**Доказательство:** исходная модель разрешает `UNIQUE(account_id, operation_id)`, поздняя миграция добавляет global unique, runtime ищет только по operation ID и перезаписывает найденной строке `account_id`.

**Последствие:** миграция падает на допустимой истории либо операция молча переносится между счетами.

**Исправление:** удалить global constraint и выполнять lookup/upsert по `(account_id, operation_id)`.

**Тест:** одинаковый ID на двух accounts должен оставить две независимые строки.

### P1-11 — Неверный явно заданный `TINKOFF_ACCOUNT_ID` молча выбирает другой счёт

**Где:** `src/tracker/app.py:1343-1361`.

**Условие:** ID опечатан, удалён или недоступен в ответе API.

**Доказательство:** после безуспешного поиска explicit ID функция выбирает первый открытый account.

**Последствие:** tracker собирает и публикует данные не того портфеля, не сигнализируя об ошибке конфигурации.

**Исправление:** конкретный ID должен разрешать только точное совпадение. Если ID не найден, синхронизация останавливается без записи данных. При пустом значении или `auto` автоматический выбор разрешён только тогда, когда открыт ровно один счёт; при нескольких открытых счетах требуется явно указать ID.

**Тест:** неизвестный конкретный ID вызывает startup/sync error без записи данных; `auto`/пустое значение выбирает единственный открытый счёт и останавливается при двух и более открытых счетах.

### P1-12 — Авторизованный пользователь может опубликовать приватный отчёт в группе

**Где:** `src/bot/runtime.py:495-517`, `src/bot/handlers.py:145-166`, `src/bot/handlers.py:216-250`, `src/bot/handlers.py:372-395`.

**Условие:** разрешённый user вызывает команду в group/supergroup.

**Доказательство:** authorization проверяет только user ID, а ответы, PDF и dataset отправляются в `effective_chat.id`.

**Последствие:** все участники группы получают суммы портфеля и экспорт операций.

**Исправление:** сохранить существующий белый список Telegram ID и дополнительно разрешать финансовые команды только в личной переписке. Отказ должен происходить до построения тяжёлого или приватного отчёта.

**Тест:** allowed user + group chat получает отказ и ни одного документа; private chat работает.

### P1-13 — Пустой allowlist не fail-closed и использует реальные-looking defaults

**Где:** `src/bot/runtime.py:17-26`, `src/bot/bot.py:306-316`, `.env.example:25-26`.

**Условие:** `ALLOWED_USER_IDS`/`TARGET_CHAT_IDS` пропущены или ошибочно пусты.

**Доказательство:** runtime подставляет захардкоженные ID; startup валидирует token, но не запрещает implicit authorization. Example также содержит не placeholders.

**Последствие:** неверно настроенный instance может разрешить доступ/рассылку заранее известному аккаунту.

**Исправление:** убрать defaults, использовать placeholders, валидировать непустые явные allowlists и логировать только количество записей.

**Тест:** missing/empty/malformed env завершает startup до polling.

### P1-14 — Reporter отдаёт приватные PDF без аутентификации

**Где:** `src/bot/report_server.py:19-22`, `src/bot/report_server.py:109-215`, `compose.yml:121-168`.

**Условие:** любой контейнер подключён к общей или внешней `localllm_localllm` сети.

**Доказательство:** `/reports/monthly/pdf` не требует token/HMAC; service слушает `0.0.0.0` и состоит в разделяемых сетях.

**Последствие:** приватный финансовый отчёт доступен соседнему workload; каждый запрос запускает дорогой DB/Ollama/WeasyPrint pipeline.

**Исправление:** bot должен передавать отдельный служебный ключ, а reporter — проверять его до чтения тела и построения отчёта безопасным сравнением. Дополнительно ограничить сеть вызывающих контейнеров и число одновременных запросов. Пользователь этот ключ не вводит и не видит.

**Тест:** unauthenticated/wrong token — `401/403` без вызова builder; правильный token работает только из разрешённой сети.

### P2-01 — Округление плана инвестирования может создать отрицательное распределение

**Где:** `src/bot/services.py:2132-2177`.

**Условие:** малая сумма распределяется по нескольким равным классам.

**Доказательство:** каждый raw allocation округляется `ROUND_HALF_UP`, затем весь отрицательный residue вычитается из одного класса. Для `2 ₽` и четырёх долей по `25%` результат содержит `-1 ₽` в одном классе.

**Последствие:** пользователю предлагается отрицательная покупка.

**Исправление:** largest-remainder algorithm от floor allocations с раздачей только положительных остаточных рублей.

**Тест:** для всех положительных целых сумм allocations неотрицательны и точно суммируются в deposit.

### P2-02 — Проценты P&L в `/structure` используют разные знаменатели

**Где:** `src/tracker/app.py:1382-1391`, `src/bot/services.py:2425-2454`, `src/bot/services.py:2500-2512`.

**Условие:** позиция имеет ненулевой expected yield.

**Доказательство:** position percent считается к cost basis (`value - P&L`), group/total — к current value. Для value `110` и P&L `10` получаются `10%` и `9.09%` для одной и той же позиции.

**Последствие:** уровни одной структуры противоречат друг другу.

**Исправление:** агрегировать cost basis и делить group/total P&L на него.

**Тест:** одна позиция: position/group/total percentages равны; несколько позиций проверяют weighted aggregation.

### P2-03 — Историческая доходность выплаты использует будущий cost basis

**Где:** `src/tracker/app.py:1394-1410`, `src/tracker/app.py:1787-1792`, `src/tracker/app.py:1811-1817`.

**Условие:** после купона были дополнительные покупки, затем пришёл поздний налог/reconciliation.

**Доказательство:** `get_latest_cost_basis()` берёт самый новый snapshot без ограничения `snapshot_date <= event_date`.

**Последствие:** исторический `net_yield_pct` меняется из-за будущих сделок.

**Исправление:** выбирать последний snapshot as-of event timestamp/date и явно определить поведение при его отсутствии.

**Тест:** snapshot до и после события; late tax не должен переключать denominator на будущий.

### P2-04 — Отменённая операция оставляет активный income event

**Где:** `src/tracker/app.py:1677-1685`, `src/tracker/app.py:1737-1777`, `tests/test_tracker_api_resilience.py:378-412`.

**Условие:** ранее executed coupon/dividend той же операции приходит с явным canceled/non-executed state.

**Доказательство:** affected key отмечается, но reconcile строит только executed rows и итерирует только существующие keys в `income_by_key`; старый event не удаляется и не деактивируется. Текущий тест закрепляет сохранение.

**Последствие:** отчёты продолжают показывать отменённый доход.

**Исправление:** деактивировать event только при явном state transition соответствующей операции; отсутствие записи в неполном API-окне не считать доказательством отмены.

**Тест:** explicit canceled удаляет/деактивирует; узкое неполное окно не удаляет.

### P2-05 — Годовой график считает вывод средств рыночным убытком

**Где:** `src/bot/charts.py:646-706`, `src/bot/queries.py:1348-1379`, `src/bot/queries.py:1461`, `src/bot/services.py:2552`, контракт `README.md:21,372`.

**Условие:** в году есть withdrawal.

**Доказательство:** chart subtracts monthly deposits, но не withdrawals; текстовая summary использует net external flow.

**Последствие:** withdrawal-only месяц выглядит как отрицательная доходность и расходится с summary.

**Исправление:** график должен вычитать monthly net external flow; одновременно обновить README.

**Тест:** withdrawal-only и mixed-flow month дают нулевой market delta при неизменном портфеле.

### P2-06 — Crash после daily-job claim навсегда блокирует задачу

**Где:** `migrations/20260404_bot_daily_job_runs.sql:3-23`, `src/bot/queries.py:1924-1963`, `src/bot/jobs.py:154-175`.

**Условие:** процесс завершается после commit `status='started'`, но до finalize/release.

**Доказательство:** повторный claim использует `ON CONFLICT DO NOTHING`; age/status/owner не проверяются, любой conflict трактуется как completed/processed.

**Последствие:** daily/weekly/PDF рассылка за дату никогда не повторится, включая startup catch-up.

**Исправление:** lease с `attempt_id`, timestamp, heartbeat и fenced stale takeover; `completed` остаётся терминальным.

**Тест:** fresh started не крадётся, stale started перехватывается, completed не перехватывается, старый owner не finalize после takeover.

### P2-07 — Отправка уведомлений не имеет recipient-level idempotency

**Где:** `src/bot/runtime.py:352-390`, `src/bot/jobs.py:921-957`, `src/bot/jobs.py:980-1009`, `src/bot/queries.py:1771-1812`, `tests/test_iis_tax_deduction.py:108-123`.

**Условие:** timeout после принятия Telegram, два worker, либо один из нескольких recipients недоступен.

**Доказательство:** `safe_send_message()` повторяет после любого исключения; общий marker ставится только после успеха всех адресатов. Уже успешно получивший адресат получает повтор на следующем цикле.

**Последствие:** дубли финансовых уведомлений и рассинхронизация доставки.

**Исправление:** fallback только для конкретной parse-mode `BadRequest`; outbox/claim и delivery state по recipient.

**Тест:** timeout — одна попытка; parse error — один plain-text fallback; partial fan-out не повторяет успешного адресата; concurrent workers отправляют один раз.

### P2-08 — `migrate.py --check` изменяет проверяемую БД

**Где:** `src/tracker/migrate.py:90-119`, `tests/test_tracker_migrations.py:30-96`.

**Условие:** check запускается на пустой/отстающей/повреждённой схеме.

**Доказательство:** до `check_only` создаётся и commit-ится ledger, затем вызывается `Base.metadata.create_all()`.

**Последствие:** диагностическая команда мутирует источник истины и может скрыть отсутствие ORM tables.

**Исправление:** check mode должен только читать catalog и ledger; отсутствие схемы — ошибка без DDL.

**Тест:** список объектов БД идентичен до/после `run_migrations(check_only=True)`.

### P2-09 — Fallback при отсутствии `income_events` продолжает aborted transaction

**Где:** `src/bot/queries.py:263-284`, `src/bot/queries.py:312-353`, `src/bot/runtime.py:343-349`.

**Условие:** таблица отсутствует, SQL получает PostgreSQL `42P01`.

**Доказательство:** `ProgrammingError` перехватывается без rollback/savepoint; `get_taxes_for_period()` затем выполняет второй запрос в abort-сессии.

**Последствие:** вместо заявленного graceful fallback весь отчёт падает с `current transaction is aborted`.

**Исправление:** nested transaction/savepoint для optional query либо rollback перед fallback query.

**Тест:** PostgreSQL integration без `income_events`, после исключения второй SQL в той же логической операции успешен.

### P2-10 — Невалидная дата операции заменяется текущим временем

**Где:** `src/tracker/app.py:985-991`, `src/tracker/app.py:1592-1596`, `src/tracker/app.py:1836-1844`.

**Условие:** API возвращает malformed/missing mandatory date.

**Доказательство:** parser возвращает `None`, upsert подставляет `datetime.now`; watermark может прыгнуть вперёд.

**Последствие:** операция попадает в неверный период, а инкрементальный sync рискует пропустить историю.

**Исправление:** считать invalid operation date ошибкой страницы/sync, не commit-ить данные и watermark.

**Тест:** malformed date оставляет DB/watermark неизменными и даёт диагностическую ошибку без raw payload.

### P2-11 — TLS verification по умолчанию выключена

**Где:** `src/tracker/app.py:129-140` и API client path в `src/tracker/app.py:653-673`.

**Условие:** `VERIFY_SSL` отсутствует или опечатан.

**Доказательство:** default — `false`, неизвестное значение также превращается в false; bearer requests идут без проверки сертификата.

**Последствие:** token/API data уязвимы для MITM в недоверенной сети.

**Исправление:** default true, строгий boolean parser; production должен запрещать false либо требовать отдельного явного break-glass режима.

**Тест:** missing/malformed env включает verify или завершает startup; false разрешён только в явно тестовом профиле.

### P2-12 — Deploy строит неидентифицированный грязный checkout

**Где:** `.github/workflows/deploy.yml:53-65`, `.github/workflows/deploy.yml:80-85`.

**Условие:** на self-hosted runner есть не конфликтующие modified/untracked files.

**Доказательство:** dirty state только печатается; нет fail-closed clean check и подтверждения точного HEAD перед Docker build.

**Последствие:** production image не соответствует commit или проверенному CI artifact.

**Исправление:** disposable clean worktree/checkout по SHA, затем проверка HEAD, clean status и image identity.

**Тест:** намеренно грязный canonical checkout останавливает deploy до build.

### P2-13 — Startup smoke и Compose readiness дают false green

**Где:** `src/bot/proxy_smoke.py:36-52`, `src/bot/entrypoint.py:129-137`, `compose.yml:65-119`, `docs/CONFIG.md:138-144`.

**Условие:** Telegram отвечает `401`/`{"ok":false}`, либо bot/tracker входят в restart loop.

**Доказательство:** любой HTTP status считается успехом; результат smoke игнорируется; у bot/tracker нет healthcheck, поэтому `compose up --wait` не доказывает их готовность.

**Последствие:** deploy отмечается успешным при неработающем приложении.

**Исправление:** проверять `200` и `ok=true`, fail closed до polling, добавить содержательные healthchecks обоим сервисам и обновить docs.

**Тест:** fake 401/invalid JSON/ok=false дают nonzero; restart-loop service делает Compose wait красным.

### P2-14 — Логи содержат приватные сообщения и стабильные идентификаторы

**Где:** `src/bot/runtime.py:352-365`, `src/bot/runtime.py:495-515`, `src/bot/runtime.py:536-550`, `src/common/logging_setup.py:25-98`, `src/tracker/app.py:551-565`, `src/tracker/app.py:737-774`.

**Условие:** пользователь вызывает команду/пишет текст либо API отвечает ошибкой с payload.

**Доказательство:** bot пишет preview/full message, username, user/chat IDs; tracker сериализует response body. Sanitizer ориентирован на secret names и не редактирует финансовый текст/PII/raw payload.

**Последствие:** персональные и финансовые данные попадают в долговечные логи.

**Исправление:** логировать command/type/length/status, pseudonymized IDs; allowlist безопасных API fields, строгий truncation и redaction.

**Тест:** guardrail проверяет отсутствие raw message, username, chat/user ID и произвольных response fields.

### P2-15 — Reporter допускает resource-exhaustion через unbounded threaded server

**Где:** `src/bot/report_server.py:34-36`, `src/bot/report_server.py:65-78`, `src/bot/report_server.py:122-215`.

**Условие:** доступный клиент открывает много медленных/параллельных соединений.

**Доказательство:** `ThreadingHTTPServer` создаёт потоки без общего лимита; нет server-side concurrency budget/connection deadline, а каждый запрос запускает тяжёлый pipeline.

**Последствие:** истощение CPU/RAM/DB connections и деградация bot.

**Исправление:** после обязательной auth добавить bounded worker/semaphore, socket/body deadline, request budget и rate limit.

**Тест:** сверх лимита возвращается `429/503`, builder concurrency не превышает budget, slow client освобождается по timeout.

### P2-16 — Xray SOCKS proxy открыт всей default Docker-сети без auth

**Где:** `src/xray_client/render_config.py:102-157`, `compose.yml:22-44`.

**Условие:** другой container подключён к default project network.

**Доказательство:** inbound слушает `0.0.0.0`, no-auth; network isolation для единственного bot caller отсутствует.

**Последствие:** соседний workload получает неучтённый egress через proxy.

**Исправление:** отдельная `internal` сеть только bot+xray и/или authenticated inbound; не публиковать порт host-side.

**Тест:** разрешённый bot соединяется, посторонний test container из default network — нет.

### P2-17 — Xray binary устанавливается без проверки checksum/signature

**Где:** `docker/Dockerfile.xray-client:17-28`.

**Условие:** upstream artifact/CDN/DNS скомпрометирован или archive повреждён.

**Доказательство:** release zip скачивается и распаковывается без сверки digest/signature.

**Последствие:** произвольный бинарник попадает в runtime image.

**Исправление:** pin version и SHA-256 (лучше signature), проверять до unzip; обновление — отдельный reviewable diff.

**Тест:** неверный checksum гарантированно ломает build.

### P2-18 — Известный пароль БД используется как runtime fallback

**Где:** `src/tracker/app.py:142-152`, `src/bot/runtime.py:145-155`.

**Условие:** `DB_PASSWORD`/`DB_DSN` не заданы.

**Доказательство:** приложение молча строит DSN с захардкоженным паролем.

**Последствие:** забытая конфигурация запускается с предсказуемыми credentials вместо безопасного отказа.

**Исправление:** обязательный secret/DSN и fail-fast; сменить пароль там, где fallback когда-либо использовался.

**Тест:** missing credentials завершают startup до подключения и не печатают DSN.

### P2-19 — `/history` и `/twr` используют общие постоянные файлы и не удаляют их

**Где:** `src/bot/handlers.py:406-429`, `src/bot/handlers.py:430-471`.

**Условие:** два запроса выполняются одновременно либо после ответа читается `/tmp`.

**Доказательство:** пути фиксированы (`/tmp/history.png`, `/tmp/twr.png`), cleanup отсутствует.

**Последствие:** конкурентная перезапись/чужой график и остаточные приватные изображения.

**Исправление:** `NamedTemporaryFile`/`TemporaryDirectory` с уникальным путём, permissions `0600`, cleanup в `finally`.

**Тест:** параллельные handlers получают разные файлы; после успеха и exception файлов нет.

### P2-20 — Синхронные тяжёлые операции блокируют asyncio event loop bot

**Где:** `src/bot/handlers.py:145-166`, `src/bot/handlers.py:293-395`, `src/bot/handlers.py:406-471`.

**Условие:** строится chart/dataset или выполняется медленный DB query одновременно с другими updates.

**Доказательство:** async handlers вызывают синхронные DB/chart/archive функции напрямую; только часть PDF-пути вынесена из event loop.

**Последствие:** polling, callbacks и уведомления зависают на время тяжёлой команды.

**Исправление:** `asyncio.to_thread`/ограниченный executor для blocking work, отдельные DB sessions внутри worker, per-command timeout/concurrency budget.

**Тест:** slow fake builder не блокирует heartbeat/вторую команду; превышение timeout корректно освобождает ресурсы.

### P2-21 — Частично успешная плановая рассылка помечается полностью завершённой

**Где:** `src/bot/jobs.py:150-157`, `src/bot/jobs.py:196-224` и использующие этот итог scheduled send loops.

**Условие:** рассылка имеет несколько target chats; хотя бы одному сообщение отправлено, хотя бы одному — нет.

**Доказательство:** run освобождается для retry только при `sent_total == 0 and failed_total > 0`; любой частичный успех приводит к finalize общего run. Это отличается от P2-07: здесь теряются scheduled reports/jobs, а не только event notifications.

**Последствие:** неуспешный получатель теряет сообщение навсегда, потому что общая уникальная запись блокирует повторный запуск.

**Исправление:** delivery ledger по `(job_name, run_date, chat_id, message_type)`; общий run завершать только после терминального результата каждого адресата, повторять только недоставленных.

**Тест:** два recipients, success/failure; следующий запуск отправляет только второму и лишь затем завершает общий run.

### P3-01 — Зависимости и GitHub Actions недостаточно закреплены

**Где:** `requirements/*.txt`, `.github/workflows/*.yml`.

Большинство Python dependencies не pin-нуты на exact versions, actions используются по mutable major tags. Это повышает риск нерепродуцируемых сборок и supply-chain drift. Нужны lock/hashes и immutable action SHA с контролируемым обновлением.

### P3-02 — Debug artifacts отчёта могут бессрочно сохранять финансовые данные

**Где:** `src/bot/report_payload.py:1253-1261`, `src/bot/report_payload.py:1502-1511`, `src/bot/report_render.py:1830-1862`.

При включённых debug flags payload/HTML создаются как persistent temp files без lifecycle cleanup. Нужны явный защищённый debug directory, retention/cleanup и предупреждение об уровне чувствительности.

### P3-03 — CSV dataset не нейтрализует spreadsheet formulas

**Где:** CSV serialization path `src/bot/runtime.py:470-486` и экспортируемые строковые поля dataset.

Если broker-provided name/description начинается с `=`, `+`, `-` или `@`, открытие CSV в spreadsheet может интерпретировать значение как формулу. Для CSV, предназначенного для Excel/Sheets, потенциально опасные ячейки следует экранировать либо явно документировать JSON как безопасный основной формат.

## Покрытие и зоны без новых дефектов

В ходе шести проходов просмотрены все tracked Python-файлы в `src` и `tests`, миграции, Docker/Compose, GitHub workflows, entrypoints, config examples и активная документация. Дополнительно проверены сквозные контракты account/date/currency/notification/deploy.

В просмотренной реализации не найдено дополнительных подтверждённых дефектов в следующих механизмах:

- сортировка миграций и исключение `*.rollback.sql`;
- checksum уже применённых миграций и advisory lock apply-пути;
- rollback текущей миграции при SQL-ошибке;
- fail-closed защита курсорной pagination от повторов/зацикливания/лимита страниц;
- сохранение вручную назначенного `cashflow_category` при API upsert;
- account-scoping snapshots, positions и payout calendar;
- уникальность snapshot `(account_id, snapshot_date)`;
- Xray runtime health/failover tests в пределах их текущего контракта;
- bounds на размер reporter request body (это не устраняет отсутствие auth/concurrency limits).

## Выполненные проверки

| Проверка | Результат |
|---|---|
| `python -m compileall src` | Не запущена: команда `python` отсутствует |
| `python3 -m compileall -q src` | PASS |
| `docker compose config --quiet` | PASS |
| `git diff --check` до создания отчёта | PASS |
| Полный `python3 -m unittest discover -s tests -p 'test_*.py'` | Не является PASS: обнаружено 106 тестов, 16 import errors из-за отсутствующих `telegram`, `sqlalchemy`, `matplotlib`, `httpx`; assertion failures не наблюдались |
| Focused dependency-free suite | PASS: 81 тест |

Focused suite включал reporting metrics, rebalance helpers, dataset/report payload helpers, monthly position diff, income calculations, scheduler/catch-up pure logic, startup resilience, text utilities и Xray config.

Не выполнялись:

- PostgreSQL integration и применение/rollback миграций на disposable DB;
- Docker build/up и health/runtime acceptance;
- live Telegram, T-Invest, Ollama, VLESS и reporter requests;
- GitHub Actions/deploy на self-hosted runner;
- `ruff`, `mypy`, `bandit`, `semgrep` — инструменты отсутствуют в локальном окружении.

## Утверждённые поведенческие решения

Статус: все восемь решений утверждены владельцем проекта 2026-08-13. Последующие исправления, тесты и документация должны реализовывать именно эти правила; агент не вправе молча менять их семантику.

1. **Расчёт результата портфеля.** Из изменения стоимости вычитаются только внешние пополнения и выводы. Купоны, дивиденды, комиссии и налоги остаются составными частями результата портфеля.
2. **Налоговый вычет ИИС.** Классификация остаётся ручной и обратимой. Сумма вычета исключается из собственных вкладов, выполнения годового плана, внешних потоков TWR и вложенного капитала XIRR, но включается в доход портфеля и не уменьшает его стоимость.
3. **Несколько валют.** Суммы сначала показываются отдельно по валютам. Неявно складывать, например, рубли и доллары запрещено. Пересчёт в одну валюту допускается только отдельным изменением с утверждённым источником курса и датой курса.
4. **Дата и время.** Моменты операций хранятся в UTC. Пользовательские отчётные периоды определяются в `TIMEZONE`; их локальные полуинтервалы `[начало, конец)` перед запросом к базе переводятся в UTC.
5. **Налоги.** Удержанный налог отображается как положительный расход. Возврат налога учитывается отдельной категорией и не кодируется простым изменением знака удержания.
6. **Отмена выплаты.** Купон или дивиденд удаляется либо помечается неактивным только при явном состоянии отмены соответствующей операции. Отсутствие операции в неполном окне ответа брокера не является доказательством отмены.
7. **Доступ к финансовым данным.** Существующий белый список Telegram ID сохраняется. Финансовые команды, документы, графики и кнопки разрешены только в личной переписке с ботом; вызов в группе отклоняется до формирования данных. Внутренняя служба PDF принимает запрос только от бота со служебным ключом; пользователь ключ не вводит и не видит.
8. **Выбор брокерского счёта.** Конкретный `TINKOFF_ACCOUNT_ID` допускает только точное совпадение; если счёт не найден, синхронизация останавливается без записи. При пустом значении или `auto` автоматически выбирается единственный открытый счёт; если открытых счетов несколько, требуется указать точный ID.

## Рекомендуемый порядок исправления

1. **Немедленно:** ротация VLESS credentials, затем sanitization repository/history.
2. **Остановить unsafe deploy:** связать deploy с green CI и точным clean SHA.
3. **Восстановить основную функцию:** исправить notifier/query contract и IIS markup.
4. **Защитить данные:** private-chat authorization, явные allowlists, reporter auth/network isolation, redaction логов.
5. **Исправить финансовое ядро по утверждённым выше контрактам:** timezone, currency, external/internal flows, taxes, TWR/P&L; одновременно обновить `README.md`/`docs/CONFIG.md`.
6. **Исправить целостность:** account-scoped operation key, strict account selection, invalid-date fail-closed, migration check/fallback transaction.
7. **Устранить delivery/runtime races:** leases/fencing, recipient-level outbox, bounded reporter/executor, unique temp files.
8. **Добавить regression/integration gates:** PostgreSQL, orchestration jobs, workflow contracts, Docker readiness и secret scanning.

## Критерий готовности после исправлений

Версию можно считать кандидатом на deploy только когда:

- все `P0/P1` закрыты тестами;
- полный unit suite проходит в зафиксированном окружении без import errors;
- disposable PostgreSQL migration/app integration проходит с account/date/currency fixtures;
- Docker Compose runtime подтверждает health bot/tracker/reporter/xray;
- deploy использует ровно SHA успешного CI и exact image identity;
- secret scan чист, а скомпрометированные credentials действительно ротированы.
