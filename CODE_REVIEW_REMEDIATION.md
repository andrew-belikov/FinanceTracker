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
| P0-01 | IR-00 | security | secret scan текущих файлов и Git history; синтетические VLESS fixtures | PR #27, #33 | REVIEWED | 317/317; tracked tree 0; history 0 | finance: repository part APPROVE | старая VLESS identity отсутствует; новая активна на обоих маршрутах `ru_hop`; homeserver xray healthy; значения не раскрывались |
| P1-01 | PR-03 | notifications | точные query-row contracts для income/invest jobs и callback markup | PR #30 | REVIEWED | 317/317; row/markup contracts PASS | security APPROVE | bot job smoke без `KeyError`; кнопка только у пополнения |
| P1-02 | PR-02 | ci-deploy | workflow contract: green CI и равенство CI/deploy SHA | PR #28 | REVIEWED | 317/317; workflow contracts PASS | security APPROVE | CI/deploy run одного SHA |
| P1-03 | PR-07 | data | table-driven day/month/year/DST local bounds → UTC `[start,end)` | PR #31, #33 | REVIEWED | 317/317; timezone/DST и year-chart boundary contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 подтвердил локальную границу года; production pending |
| P1-04 | PR-08 | finance | withholding `-13` + operation tax `-5` = expense `18`; tax refund отдельно | PR #29 | REVIEWED | 317/317; tax/refund contracts PASS | ci APPROVE | очищенный отчётный smoke |
| P1-05 | PR-09 | finance | gap-day deposit/withdrawal, несколько потоков и `/today` | PR #29 | REVIEWED | 317/317; gap-day contracts PASS | ci APPROVE | TWR/today smoke на тестовых данных |
| P1-06 | PR-09 | finance | pre-period return + flat selected period = `0%` | PR #29 | REVIEWED | 317/317; period rebase PASS | ci APPROVE | monthly payload/PDF smoke |
| P1-07 | PR-08 | finance | dividend/commission/tax/deposit/withdrawal и fallback без start snapshot | PR #29 | REVIEWED | 317/317; P&L contracts PASS | ci APPROVE | daily/monthly payload smoke |
| P1-08 | PR-08 | finance | deposit/withdrawal round-trip и несколько выводов | PR #29 | REVIEWED | 317/317; lifetime P&L PASS | ci APPROVE | `/today` безопасный smoke владельцем |
| P1-09 | PR-07 | data | mixed RUB/USD income/tax; неизвестная валюта без неявного сложения | PR #31, #33 | REVIEWED | 317/317; RUB/USD/UNKNOWN breakdown и base-scalar contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 подтвердил раздельные income/tax/refund; production pending |
| P1-10 | PR-10 | data | одинаковый `operation_id` на двух accounts | PR #31 | REVIEWED | 317/317; PostgreSQL 16 composite identity/collision rollback PASS | finance APPROVE | migration и sync ledger |
| P1-11 | PR-10 | data | exact ID absent; `auto` с 0/1/2 open accounts | PR #31 | REVIEWED | 317/317; exact/auto account contracts PASS | finance APPROVE | tracker sync без account fallback |
| P1-12 | PR-05 | security | allowed user в group отклоняется до DB/PDF/dataset/chart | PR #27 | REVIEWED | 317/317; private-chat contracts PASS | finance APPROVE | отрицательный private-data smoke |
| P1-13 | PR-05 | security | missing/empty/malformed allowlist останавливает startup | PR #27 | REVIEWED | 317/317; config fail-fast PASS | finance APPROVE | startup config evidence без ID |
| P1-14 | PR-06 | security | no/wrong key → `401/403` до body/builder; correct key succeeds | PR #32 | REVIEWED | 317/317; live auth ordering PASS | ci APPROVE | reporter negative/authorized smoke |
| P2-01 | PR-11 | finance | property cases: allocations >= 0 и сумма равна deposit | PR #29 | REVIEWED | 317/317; allocation properties PASS | ci APPROVE | N/A после unit evidence |
| P2-02 | PR-11 | finance | единый cost-basis denominator для position/group/total | PR #29 | REVIEWED | 317/317; cost-basis contracts PASS | ci APPROVE | `/structure` безопасный smoke владельцем |
| P2-03 | PR-10 | data | cost basis snapshot до/после event; late tax сохраняет as-of basis | PR #31 | REVIEWED | 317/317; as-of basis PASS | finance APPROVE | reconciliation smoke |
| P2-04 | PR-10 | data | explicit canceled деактивирует; неполное окно не деактивирует | PR #31 | REVIEWED | 317/317; sparse cancel/partial window PASS | finance APPROVE | tracker reconciliation smoke |
| P2-05 | PR-08 | finance | withdrawal-only и mixed-flow month | PR #29, #33 | REVIEWED | 317/317; year-chart и first-month boundary contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 boundary evidence; production year-chart smoke pending |
| P2-06 | PR-04 | notifications | fresh/stale/completed lease и fenced finalize старого owner | PR #30, #33 | REVIEWED | 317/317; lease/fencing/stale pre-send recovery PASS | adversarial final APPROVE | disposable PostgreSQL 16 lease/fencing evidence; production scheduler pending |
| P2-07 | PR-04 | notifications | timeout без retry; parse error один fallback; recipient idempotency/concurrency | PR #30, #33 | REVIEWED | 317/317; timeout/BadRequest/partial/stale recovery contracts PASS | adversarial final APPROVE | disposable PostgreSQL 16 сохранил terminal `uncertain`; production delivery pending |
| P2-08 | PR-10 | data | catalog/ledger до и после `migrate --check` идентичны | PR #31 | REVIEWED | 317/317; PostgreSQL 16 empty/lagging/current checksum PASS | finance APPROVE | production `migrate --check` read-only |
| P2-09 | PR-10 | data | PostgreSQL без `income_events`: savepoint/fallback продолжает запросы | PR #31 | REVIEWED | 317/317; PostgreSQL 16 savepoint/follow-up SQL PASS | finance APPROVE | PostgreSQL integration evidence |
| P2-10 | PR-10 | data | malformed date не меняет DB/watermark и не логирует payload | PR #31 | REVIEWED | 317/317; page rollback/watermark PASS | finance APPROVE | tracker sync без date errors |
| P2-11 | PR-12 | security | missing/malformed `VERIFY_SSL`; false только explicit test profile | PR #27 | REVIEWED | 317/317; TLS config contracts PASS | finance APPROVE | production config-path evidence без значения секрета |
| P2-12 | PR-02 | ci-deploy | dirty canonical checkout останавливается до build; exact HEAD/image | PR #28 | REVIEWED | 317/317; exact-image contracts PASS | security APPROVE | clean exact-SHA deployment |
| P2-13 | PR-02 | ci-deploy | HTTP 401/invalid JSON/`ok=false`; bot/tracker health readiness | PR #28 | REVIEWED | 317/317; readiness contracts PASS | security APPROVE | healthy services без restart loop |
| P2-14 | PR-12 | security | logs не содержат raw message/username/stable IDs/upstream payload | PR #27 | REVIEWED | 317/317; adversarial redaction PASS | finance APPROVE | очищенный ERROR/CRITICAL scan |
| P2-15 | PR-06 | security | concurrency budget, overload response и slow-client timeout | PR #32 | REVIEWED | 317/317; live concurrency/slow-body PASS | ci APPROVE | reporter load/health smoke |
| P2-16 | PR-06 | security | bot видит SOCKS, контейнер default network не видит | PR #32 | REVIEWED | 317/317; disposable Docker isolation PASS/cleaned | ci APPROVE | Docker network isolation evidence |
| P2-17 | PR-12 | security | неверный Xray SHA-256 ломает build | PR #27 | REVIEWED | 317/317; bad SHA fails before unzip; pinned amd64 build PASS | ci APPROVE | image build identity |
| P2-18 | PR-05 | security | missing DB secret fail-fast без DSN в логах | PR #27, #28 | REVIEWED | 317/317; entrypoint fail-fast PASS | ci APPROVE | startup config evidence без секрета |
| P2-19 | PR-13 | security | concurrent history/TWR paths уникальны; cleanup success/error | PR #32 | REVIEWED | 317/317; concurrent/error cleanup PASS | ci APPROVE | temp-artifact absence после smoke |
| P2-20 | PR-13 | security | slow builder не блокирует heartbeat; timeout освобождает budget | PR #32 | REVIEWED | 317/317; heartbeat/budget/late cleanup PASS | ci APPROVE | bot responsiveness smoke |
| P2-21 | PR-04 | notifications | partial recipients: retry только failed, затем complete | PR #30 | REVIEWED | 317/317; recipient ledger contracts PASS | security APPROVE | disposable PostgreSQL 16 recipient ledger/partial retry PASS; production pending |
| P3-01 | PR-01 | ci-deploy | clean locked install с hashes; action refs immutable | PR #28 | REVIEWED | locked Python 3.12; 317/317; hash contracts PASS | security APPROVE | CI exact dependency evidence |
| P3-02 | PR-13 | security | protected debug dir, retention и cleanup | PR #32 | REVIEWED | 317/317; 1000-write race + symlink/traversal PASS | ci APPROVE | отсутствие persistent debug artifacts |
| P3-03 | PR-13 | security | CSV cells `=`, `+`, `-`, `@` нейтрализуются | PR #32 | REVIEWED | 317/317; CSV Cc/Cf neutralized; JSON raw PASS | ci APPROVE | dataset archive inspection без данных пользователя |

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
history выполнены отдельно до merge: старая identity удалена, новый маршрут
healthy, current/history scan не содержит находок. Значения credentials в
доказательствах не сохраняются.

До отдельного разрешения владельца по-прежнему запрещены merge в `main` и
production deploy remediation-кода. После такого разрешения production-
доказательства собираются только для точного CI-проверенного SHA, с новой
no-clobber резервной копией, безопасным isolated restore при возможности и без
изменения посторонних сервисов, данных или Docker volumes.
