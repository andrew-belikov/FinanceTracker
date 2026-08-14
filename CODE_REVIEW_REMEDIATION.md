# Устранение замечаний CODE_REVIEW.md

Дата начала: 2026-08-13
Базовый commit аудита после очистки history: `53a5f0409484a0712cc3fa388ab79ccb35d80b8d`
Источник доказательств: `CODE_REVIEW.md`

Этот документ отслеживает исправления отдельно от исходного протокола аудита.
Текст findings в `CODE_REVIEW.md` не редактируется и остаётся источником
первоначальных доказательств.

## Статусы

- `CONFIRMED` — путь исполнения и дефект подтверждены аудитом.
- `IN_PROGRESS` — написан воспроизводящий тест или выполняется исправление.
- `LOCAL_PASS` — focused и затронутые integration-проверки прошли.
- `REVIEWED` — diff одобрен независимым агентом.
- `PR_READY` — ветка обновлена от актуальной базы, PR и доказательства готовы.
- `PROD_VERIFIED` — точный SHA развёрнут и безопасно проверен в production.
- `EXTERNAL_BLOCKED` — требуется действие владельца вне репозитория.
- `RISK_ACCEPTED` — исправление применено и credential отозван, но владелец
  явно принял ограниченный внешний остаток, который штатный Git push не удаляет.

Finding закрывается только после автоматического доказательства и независимого
review. Статус `PROD_VERIFIED` применяется только к изменениям, имеющим runtime,
deployment, migration или security-последствия.

## Неизменяемые поведенческие решения

1. Из изменения стоимости вычитаются только внешние пополнения и выводы;
   купоны, дивиденды, комиссии и налоги остаются в результате портфеля.
2. Вычет ИИС размечается вручную и обратимо; он исключается из собственных
   вкладов, годового плана, внешних потоков TWR и вложенного капитала XIRR, но
   входит в доход портфеля.
3. Валюты показываются отдельно; неявное сложение и новый FX-пересчёт запрещены.
4. Моменты операций хранятся в UTC; локальные периоды `TIMEZONE` преобразуются
   из полуинтервала `[start, end)` в UTC.
5. Удержанный налог — положительный расход; возврат налога — отдельная категория.
6. Выплата отменяется только по явному состоянию отмены операции.
7. Белый список Telegram ID сохраняется; финансовые функции доступны только в
   private chat; reporter принимает финансовые запросы только от bot по ключу.
8. Явный `TINKOFF_ACCOUNT_ID` требует точного совпадения; `auto`/пустое значение
   выбирает только единственный открытый счёт.

## Матрица findings

В колонках `PR / commit`, `Локальные проверки`, `Независимое review` и
`Production-доказательство` значение `pending` заменяется точной ссылкой или
результатом по мере прохождения gates.

| ID | Пакет | Ответственный агент | Воспроизводящий тест / автоматическая проверка | PR / commit | Статус | Локальные проверки | Независимое review | Production-доказательство |
|---|---|---|---|---|---|---|---|---|
| P0-01 | IR-00 | security | secret scan текущих файлов и Git history; синтетические VLESS fixtures | PR #27, #33 | RISK_ACCEPTED: branches/local clean; GitHub `refs/pull/1..25/head` оставлены по явному решению владельца | 320/320; tracked/local history 0; fresh GitHub mirror: 2 DB fallback findings только в read-only pull refs | finance: repository part APPROVE | VLESS identity заменена; legacy DB fallback отозван ротацией; local/production env синхронизированы; DB/app probes и xray health PASS; значения не раскрывались |
| P1-01 | PR-03 | notifications | точные query-row contracts для income/invest jobs и callback markup | PR #30 | REVIEWED | 320/320; row/markup contracts PASS | security APPROVE | bot job smoke без `KeyError`; кнопка только у пополнения |
| P1-02 | PR-02 | ci-deploy | workflow contract: green CI и равенство CI/deploy SHA | PR #28 | REVIEWED | 320/320; workflow contracts PASS | security APPROVE | CI/deploy run одного SHA |
| P1-03 | PR-07 | data | table-driven day/month/year/DST local bounds → UTC `[start,end)` | PR #31, #33 | PROD_VERIFIED | 320/320; timezone/DST и year-chart boundary contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 подтвердил локальную границу года; production year и monthly-delta charts построены через canonical bounds и сразу удалены |
| P1-04 | PR-08 | finance | withholding `-13` + operation tax `-5` = expense `18`; tax refund отдельно | PR #29 | REVIEWED | 320/320; tax/refund contracts PASS | ci APPROVE | очищенный отчётный smoke |
| P1-05 | PR-09 | finance | gap-day deposit/withdrawal, несколько потоков и `/today` | PR #29 | REVIEWED | 320/320; gap-day contracts PASS | ci APPROVE | TWR/today smoke на тестовых данных |
| P1-06 | PR-09 | finance | pre-period return + flat selected period = `0%` | PR #29 | REVIEWED | 320/320; period rebase PASS | ci APPROVE | monthly payload/PDF smoke |
| P1-07 | PR-08 | finance | dividend/commission/tax/deposit/withdrawal и fallback без start snapshot | PR #29 | REVIEWED | 320/320; P&L contracts PASS | ci APPROVE | daily/monthly payload smoke |
| P1-08 | PR-08 | finance | deposit/withdrawal round-trip и несколько выводов | PR #29 | REVIEWED | 320/320; lifetime P&L PASS | ci APPROVE | `/today` безопасный smoke владельцем |
| P1-09 | PR-07 | data | mixed RUB/USD income/tax; неизвестная валюта без неявного сложения | PR #31, #33 | PROD_VERIFIED | 320/320; RUB/USD/UNKNOWN breakdown и base-scalar contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 подтвердил mixed RUB/USD/UNKNOWN; production breakdown выполнился и вернул только фактически присутствующий RUB без UNKNOWN/неявного FX |
| P1-10 | PR-10 | data | одинаковый `operation_id` на двух accounts | PR #31 | REVIEWED | 320/320; PostgreSQL 16 composite identity/collision rollback PASS | finance APPROVE | migration и sync ledger |
| P1-11 | PR-10 | data | exact ID absent; `auto` с 0/1/2 open accounts | PR #31 | PROD_VERIFIED | 320/320; exact/auto account contracts PASS | finance APPROVE | production `auto` fail-closed при 7 open accounts; единственный исторический open account закреплён exact, local/server fingerprint совпадает; tracker healthy без рестартов |
| P1-12 | PR-05 | security | allowed user в group отклоняется до DB/PDF/dataset/chart | PR #27 | REVIEWED | 320/320; private-chat contracts PASS | finance APPROVE | отрицательный private-data smoke |
| P1-13 | PR-05 | security | missing/empty/malformed allowlist останавливает startup | PR #27 | REVIEWED | 320/320; config fail-fast PASS | finance APPROVE | startup config evidence без ID |
| P1-14 | PR-06 | security | no/wrong key → `401/403` до body/builder; correct key succeeds; reporter import не требует Telegram package | PR #32, #33; `6437a8d` | PROD_VERIFIED | 320/320; live auth ordering и dependency boundary PASS; минимальный reporter image без `telegram` импортируется | ci APPROVE | exact reporter healthy; `/healthz=200`, missing key `401`, wrong key `403`, valid key + invalid body `400` до build |
| P2-01 | PR-11 | finance | property cases: allocations >= 0 и сумма равна deposit | PR #29 | REVIEWED | 320/320; allocation properties PASS | ci APPROVE | N/A после unit evidence |
| P2-02 | PR-11 | finance | единый cost-basis denominator для position/group/total | PR #29 | REVIEWED | 320/320; cost-basis contracts PASS | ci APPROVE | `/structure` безопасный smoke владельцем |
| P2-03 | PR-10 | data | cost basis snapshot до/после event; late tax сохраняет as-of basis | PR #31 | REVIEWED | 320/320; as-of basis PASS | finance APPROVE | reconciliation smoke |
| P2-04 | PR-10 | data | explicit canceled деактивирует; неполное окно не деактивирует | PR #31 | REVIEWED | 320/320; sparse cancel/partial window PASS | finance APPROVE | tracker reconciliation smoke |
| P2-05 | PR-08 | finance | withdrawal-only и mixed-flow month | PR #29, #33 | PROD_VERIFIED | 320/320; year-chart и first-month boundary contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 boundary evidence; production year и monthly-delta PNG smoke PASS, временные файлы удалены |
| P2-06 | PR-04 | notifications | fresh/stale/completed lease и fenced finalize старого owner | PR #30, #33 | PROD_VERIFIED | 320/320; lease/fencing/stale pre-send recovery PASS | adversarial final APPROVE | disposable PostgreSQL 16 lease/fencing PASS; production scheduler стартовал без ERROR/CRITICAL и restart loop |
| P2-07 | PR-04 | notifications | timeout без retry; parse error один fallback; recipient idempotency/concurrency | PR #30, #33 | PROD_VERIFIED | 320/320; timeout/BadRequest/partial/stale recovery contracts PASS | adversarial final APPROVE | production ledger сохранил ровно 4 `sent` delivery для двух backlog events × двух recipients; повторный bot restart не добавил доставок |
| P2-08 | PR-10 | data | catalog/ledger до и после `migrate --check` идентичны | PR #31 | PROD_VERIFIED | 320/320; PostgreSQL 16 empty/lagging/current checksum PASS | finance APPROVE | production exact tracker image: `migrate --check` exit 0 после forward migrations |
| P2-09 | PR-10 | data | PostgreSQL без `income_events`: savepoint/fallback продолжает запросы | PR #31 | REVIEWED | 320/320; PostgreSQL 16 savepoint/follow-up SQL PASS | finance APPROVE | PostgreSQL integration evidence |
| P2-10 | PR-10 | data | malformed date не меняет DB/watermark и не логирует payload | PR #31 | REVIEWED | 320/320; page rollback/watermark PASS | finance APPROVE | tracker sync без date errors |
| P2-11 | PR-12 | security | missing/malformed `VERIFY_SSL`; false только explicit test profile | PR #27 | REVIEWED | 320/320; TLS config contracts PASS | finance APPROVE | production config-path evidence без значения секрета |
| P2-12 | PR-02 | ci-deploy | dirty canonical checkout останавливается до build; exact HEAD/image | PR #28, #33 | PROD_VERIFIED | 320/320; exact-image contracts PASS | security APPROVE | clean detached `6437a8d`; CI push `31808038338` и PR `31808041779`; затронутые bot/reporter images имеют exact revision label |
| P2-13 | PR-02 | ci-deploy | HTTP 401/invalid JSON/`ok=false`; bot/tracker health readiness | PR #28 | PROD_VERIFIED | 320/320; readiness contracts PASS | security APPROVE | db/bot/tracker/reporter/xray healthy, migrate exit 0, restart count 0; bot startup smoke и tracker initial sync PASS |
| P2-14 | PR-12 | security | logs не содержат raw message/username/stable IDs/upstream payload | PR #27 | PROD_VERIFIED | 320/320; adversarial redaction PASS | finance APPROVE | последние 300 строк bot/tracker/reporter/xray: совпадений со значениями server secrets 0; ERROR/CRITICAL после финального старта 0 |
| P2-15 | PR-06 | security | concurrency budget, overload response и slow-client timeout | PR #32 | REVIEWED | 320/320; live concurrency/slow-body PASS | ci APPROVE | reporter load/health smoke |
| P2-16 | PR-06 | security | bot видит SOCKS, контейнер default network не видит | PR #32 | PROD_VERIFIED | 320/320; disposable Docker isolation PASS/cleaned | ci APPROVE | production bot/xray и bot/reporter internal-сети содержат по 2 контейнера; xray отсутствует в default; отдельный egress содержит только xray |
| P2-17 | PR-12 | security | неверный Xray SHA-256 ломает build | PR #27 | REVIEWED | 320/320; bad SHA fails before unzip; pinned amd64 build PASS | ci APPROVE | image build identity |
| P2-18 | PR-05 | security | missing DB secret fail-fast без DSN в логах | PR #27, #28 | REVIEWED | 320/320; entrypoint fail-fast PASS | ci APPROVE | startup config evidence без секрета |
| P2-19 | PR-13 | security | concurrent history/TWR paths уникальны; cleanup success/error | PR #32 | REVIEWED | 320/320; concurrent/error cleanup PASS | ci APPROVE | temp-artifact absence после smoke |
| P2-20 | PR-13 | security | slow builder не блокирует heartbeat; timeout освобождает budget | PR #32 | REVIEWED | 320/320; heartbeat/budget/late cleanup PASS | ci APPROVE | bot responsiveness smoke |
| P2-21 | PR-04 | notifications | partial recipients: retry только failed, затем complete | PR #30 | PROD_VERIFIED | 320/320; recipient ledger contracts PASS | security APPROVE | disposable PostgreSQL 16 partial retry PASS; production recipient ledger остаётся terminal `sent` без duplicate после restart |
| P3-01 | PR-01 | ci-deploy | clean locked install с hashes; action refs immutable | PR #28, #33 | PROD_VERIFIED | locked Python 3.12; 320/320; hash contracts PASS | security APPROVE | exact `6437a8d` успешно прошёл push и PR CI; server build выполнен из clean detached checkout того же SHA |
| P3-02 | PR-13 | security | protected debug dir, retention и cleanup | PR #32 | REVIEWED | 320/320; 1000-write race + symlink/traversal PASS | ci APPROVE | отсутствие persistent debug artifacts |
| P3-03 | PR-13 | security | CSV cells `=`, `+`, `-`, `@` нейтрализуются | PR #32 | REVIEWED | 320/320; CSV Cc/Cf neutralized; JSON raw PASS | ci APPROVE | dataset archive inspection без данных пользователя |

## Обязательная объединённая приёмка

- `python3 -m unittest discover -s tests -p 'test_*.py'` без import errors;
- `python3 -m compileall src`;
- `docker compose config --quiet` без публикации полного rendered config;
- `git diff --check`;
- secret scan текущих файлов и Git history;
- workflow contract checks;
- PostgreSQL integration и миграции на пустой, актуальной, отстающей БД и БД
  с одинаковыми `operation_id` на разных счетах;
- повторный migration run и доказательство read-only `migrate --check`;
- Docker runtime acceptance и отрицательные auth/network checks;
- конкурентные jobs/reporter tests;
- timezone, currency, TWR и P&L сценарии из аудита.

## Production gate

Ротация скомпрометированной VLESS identity и согласованное переписывание Git
history веток выполнены отдельно до merge: старая identity удалена, новый
маршрут healthy, current/local history scan не содержит находок. Значения
credentials в доказательствах не сохраняются.

Свежий GitHub mirror подтвердил серверный остаток: read-only
`refs/pull/1..25/head` старых закрытых PR сохраняют два value-safe finding одного
legacy DB fallback. Ветки и `refs/pull/26..33` чисты. Обычным push эти refs не
изменяются; для удаления cached views, PR refs и server-side объектов требуется
GitHub Support purge по официальной процедуре sensitive-data removal. Владелец
явно решил пропустить удаление этого остатка после отзыва credential; риск
зафиксирован как `RISK_ACCEPTED`, а не как действующий секрет.

Сверка через `homeserver_external` без вывода значений подтвердила, что этот
legacy fallback совпадал с действующим production-паролем PostgreSQL. Пароль
роли и `.env` был атомарно заменён; `db`, `migrate`, `tracker`, `bot` и
`reporter` получили новый credential, а локальный `.env` синхронизирован и
приведён к режиму `0600`. После ротации DB probes для трёх приложений, health
`db`/`reporter`/`xray-client`, отсутствие restart loop и неизменность container
identity `xray-client` подтверждены. Старый fallback больше не аутентифицирует
production, временные файлы с credential удалены.

По отдельному разрешению владельца remediation-код развёрнут напрямую из ветки
draft PR без merge в `main`. Точный production code SHA:
`6437a8d54cd85cdc917c7ca043e314cd22c7a7c1`; GitHub Actions push-run
`31808038338` и PR-run `31808041779` завершились успешно. Серверный checkout был
detached и clean. До применения создана no-clobber резервная копия
`financetracker-predeploy-16b5b99-20260814T124354Z.dump` с SHA-256
`94d9e66418734ba3a56214cc5afec0658319b1e9a8377b89f4859e14bd969355`;
isolated PostgreSQL restore, lagging read-only precheck, forward migrations,
current postcheck и сохранность row counts/fingerprint прошли.

Во время первой попытки deploy обнаружены и закрыты два fail-closed пробела:
byte-identity четырёх исторических SQL migrations (`16b5b993`) и случайный
top-level Telegram import в минимальном reporter image (`6437a8d`). Оба сначала
получили красные regression-тесты, затем зелёные. Полный locked Python 3.12 suite
на финальном SHA: `320/320`; compileall, Compose config/env verifier, diff-check и
secret scan tracked/history — PASS.

Production acceptance на `homeserver_external`:

- `bot` и `reporter` собраны из clean exact SHA и имеют revision label
  `6437a8d54cd85cdc917c7ca043e314cd22c7a7c1`;
- `db`, `bot`, `tracker`, `reporter` и `xray-client` healthy, `migrate` завершён с
  кодом 0, restart count всех runtime-контейнеров равен 0;
- DB probes из bot/tracker/reporter и production `migrate --check` — PASS;
- reporter: health `200`, missing key `401`, wrong key `403`, valid key с
  намеренно невалидным телом `400` до report build;
- internal bot/xray и bot/reporter сети содержат только нужные пары; xray не
  подключён к default network, отдельный egress содержит только xray;
- в последних логах нет совпадений со значениями server secrets и нет новых
  ERROR/CRITICAL events;
- два backlog coupon events от 2026-08-11 были доставлены двум разрешённым
  recipients ровно один раз: ledger содержит 4 terminal `sent`, повторный bot
  restart новых delivery не создал; `/today` за 2026-08-14 корректно показывает
  нулевой доход текущего дня;
- exact image/CI/backup/acceptance manifest сохранён без секретов в
  `/home/andrey/backups/financetracker/financetracker-deployed-6437a8d-images.json`.

Merge draft PR в `main` не выполнялся: такого разрешения владелец не давал.
Production checkout старой ветки не изменялся; посторонние контейнеры, данные и
Docker volumes не затрагивались.
