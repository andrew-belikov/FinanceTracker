# Устранение замечаний CODE_REVIEW.md

Дата начала: 2026-08-13  
Базовый commit аудита: `fb512f8d7e5e7ca36c41d3048711b1b5e73bc01c`  
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
| P0-01 | IR-00 | security | secret scan текущих файлов и Git history; синтетические VLESS fixtures | pending | CONFIRMED; внешняя ротация pending | pending | adversarial | отзыв старого внешнего объекта и чистый production checkout |
| P1-01 | PR-03 | notifications | точные query-row contracts для income/invest jobs и callback markup | pending | CONFIRMED | pending | data | bot job smoke без `KeyError`; кнопка только у пополнения |
| P1-02 | PR-02 | ci-deploy | workflow contract: green CI и равенство CI/deploy SHA | pending | CONFIRMED | pending | adversarial | CI/deploy run одного SHA |
| P1-03 | PR-07 | data | table-driven day/month/year/DST local bounds → UTC `[start,end)` | pending | CONFIRMED | pending | finance | tracker sync и отчётные границы без ошибок |
| P1-04 | PR-08 | finance | withholding `-13` + operation tax `-5` = expense `18`; tax refund отдельно | pending | CONFIRMED | pending | data | очищенный отчётный smoke |
| P1-05 | PR-09 | finance | gap-day deposit/withdrawal, несколько потоков и `/today` | pending | CONFIRMED | pending | notifications | TWR/today smoke на тестовых данных |
| P1-06 | PR-09 | finance | pre-period return + flat selected period = `0%` | pending | CONFIRMED | pending | data | monthly payload/PDF smoke |
| P1-07 | PR-08 | finance | dividend/commission/tax/deposit/withdrawal и fallback без start snapshot | pending | CONFIRMED | pending | data | daily/monthly payload smoke |
| P1-08 | PR-08 | finance | deposit/withdrawal round-trip и несколько выводов | pending | CONFIRMED | pending | data | `/today` безопасный smoke владельцем |
| P1-09 | PR-07 | data | mixed RUB/USD income/tax; неизвестная валюта без неявного сложения | pending | CONFIRMED | pending | finance | tracker/report currency smoke |
| P1-10 | PR-10 | data | одинаковый `operation_id` на двух accounts | pending | CONFIRMED | pending | finance | migration и sync ledger |
| P1-11 | PR-10 | data | exact ID absent; `auto` с 0/1/2 open accounts | pending | CONFIRMED | pending | security | tracker sync без account fallback |
| P1-12 | PR-05 | security | allowed user в group отклоняется до DB/PDF/dataset/chart | pending | CONFIRMED | pending | notifications | отрицательный private-data smoke |
| P1-13 | PR-05 | security | missing/empty/malformed allowlist останавливает startup | pending | CONFIRMED | pending | notifications | startup config evidence без ID |
| P1-14 | PR-06 | security | no/wrong key → `401/403` до body/builder; correct key succeeds | pending | CONFIRMED | pending | ci-deploy | reporter negative/authorized smoke |
| P2-01 | PR-11 | finance | property cases: allocations >= 0 и сумма равна deposit | pending | CONFIRMED | pending | data | N/A после unit evidence |
| P2-02 | PR-11 | finance | единый cost-basis denominator для position/group/total | pending | CONFIRMED | pending | data | `/structure` безопасный smoke владельцем |
| P2-03 | PR-10 | data | cost basis snapshot до/после event; late tax сохраняет as-of basis | pending | CONFIRMED | pending | finance | reconciliation smoke |
| P2-04 | PR-10 | data | explicit canceled деактивирует; неполное окно не деактивирует | pending | CONFIRMED | pending | finance | tracker reconciliation smoke |
| P2-05 | PR-08 | finance | withdrawal-only и mixed-flow month | pending | CONFIRMED | pending | data | year-chart smoke владельцем |
| P2-06 | PR-04 | notifications | fresh/stale/completed lease и fenced finalize старого owner | pending | CONFIRMED | pending | ci-deploy | scheduler lease ledger |
| P2-07 | PR-04 | notifications | timeout без retry; parse error один fallback; recipient idempotency/concurrency | pending | CONFIRMED | pending | security | очищенный delivery smoke |
| P2-08 | PR-10 | data | catalog/ledger до и после `migrate --check` идентичны | pending | CONFIRMED | pending | ci-deploy | production `migrate --check` read-only |
| P2-09 | PR-10 | data | PostgreSQL без `income_events`: savepoint/fallback продолжает запросы | pending | CONFIRMED | pending | finance | PostgreSQL integration evidence |
| P2-10 | PR-10 | data | malformed date не меняет DB/watermark и не логирует payload | pending | CONFIRMED | pending | security | tracker sync без date errors |
| P2-11 | PR-12 | security | missing/malformed `VERIFY_SSL`; false только explicit test profile | pending | CONFIRMED | pending | ci-deploy | production config-path evidence без значения секрета |
| P2-12 | PR-02 | ci-deploy | dirty canonical checkout останавливается до build; exact HEAD/image | pending | CONFIRMED | pending | adversarial | clean exact-SHA deployment |
| P2-13 | PR-02 | ci-deploy | HTTP 401/invalid JSON/`ok=false`; bot/tracker health readiness | pending | CONFIRMED | pending | adversarial | healthy services без restart loop |
| P2-14 | PR-12 | security | logs не содержат raw message/username/stable IDs/upstream payload | pending | CONFIRMED | pending | adversarial | очищенный ERROR/CRITICAL scan |
| P2-15 | PR-06 | security | concurrency budget, overload response и slow-client timeout | pending | CONFIRMED | pending | ci-deploy | reporter load/health smoke |
| P2-16 | PR-06 | security | bot видит SOCKS, контейнер default network не видит | pending | CONFIRMED | pending | ci-deploy | Docker network isolation evidence |
| P2-17 | PR-12 | security | неверный Xray SHA-256 ломает build | pending | CONFIRMED | pending | ci-deploy | image build identity |
| P2-18 | PR-05 | security | missing DB secret fail-fast без DSN в логах | pending | CONFIRMED | pending | ci-deploy | startup config evidence без секрета |
| P2-19 | PR-13 | security | concurrent history/TWR paths уникальны; cleanup success/error | pending | CONFIRMED | pending | notifications | temp-artifact absence после smoke |
| P2-20 | PR-13 | security | slow builder не блокирует heartbeat; timeout освобождает budget | pending | CONFIRMED | pending | notifications | bot responsiveness smoke |
| P2-21 | PR-04 | notifications | partial recipients: retry только failed, затем complete | pending | CONFIRMED | pending | adversarial | recipient delivery ledger |
| P3-01 | PR-01 | ci-deploy | clean locked install с hashes; action refs immutable | pending | CONFIRMED | pending | security | CI exact dependency evidence |
| P3-02 | PR-13 | security | protected debug dir, retention и cleanup | pending | CONFIRMED | pending | adversarial | отсутствие persistent debug artifacts |
| P3-03 | PR-13 | security | CSV cells `=`, `+`, `-`, `@` нейтрализуются | pending | CONFIRMED | pending | finance | dataset archive inspection без данных пользователя |

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

До отдельного разрешения владельца запрещены merge в `main`, production deploy,
ротация внешних credentials и переписывание Git history. После разрешения
production-доказательства собираются только для точного CI-проверенного SHA, с
новой no-clobber резервной копией, безопасным isolated restore при возможности и
без изменения посторонних сервисов, данных или Docker volumes.
