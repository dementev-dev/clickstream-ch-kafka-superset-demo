Status: ready-for-human

# Стартовая история как источник аналитики

## Parent

`.scratch/generator-model-time-startup-history/PRD.md`

## What to build

Перевести штатный путь стенда на стартовую историю как источник аналитики.
Чистый стенд должен получать данные из генерации: стартовая история загружается,
STG→ODS→DDS→DM строится на ней, а Superset работает с этими витринами. Архивный
статический сид остаётся только временной кладовкой фактуры для генератора, а не
источником аналитического контура.

Срез не требует финальной ручной оценки красоты дашбордов, но должен дать
техническое доказательство: витрины и датасеты не пустые, контрольные числа
берутся из генерации.

Границы среза: штатный путь стенда и документы запуска. Переписывание уроков,
новые панели и улучшение формы дашбордов остаются follow-up, если по ходу не
окажутся маленькой обязательной правкой для запуска.

## Acceptance criteria

- [x] Штатная команда запуска чистого стенда создаёт или загружает стартовую
  историю и прогоняет её до DM-витрин.
- [x] `kafka_load_dag` или заменяющий его путь больше не использует архивный
  `data/*.jsonl` как источник аналитики.
- [x] ClickHouse-проверки из задачи 5 доступны как повторяемая команда для
  координатора или CI.
- [x] Основные DM-витрины, на которых стоят дашборды, непустые и показывают
  данные генерации.
- [x] Superset-датасеты и дашборды технически открываются на данных генерации;
  ручная оценка формы графиков остаётся финальной HITL-приёмкой.
- [x] `README.md`, `docs/OPERATIONS.md` и `generator/README.md` больше не
  описывают архивный сид как основной источник аналитики.
- [x] Если уроки или дашборды требуют нетривиальной переделки, создан follow-up
  issue вместо расширения этой задачи.
- [x] Если учебные материалы ещё описывают архивный сид как основной источник,
  создан отдельный follow-up на миграцию уроков по ADR-0006.
- [x] Описан повторный чистый прогон: какие данные очищаются и какие команды
  выполняются, чтобы координатор мог надёжно перепроверить результат.

## Blocked by

- `.scratch/generator-model-time-startup-history/issues/05-startup-history-backfill-to-clickhouse.md`
- Review gate из `PRD.md`: распределения и два пути генерации после задачи 5

## Фактический прогон worker-а

Дата проверки: 2026-06-14.

Штатная команда:

```bash
make generated-history-analytics
```

Команда выполняет чистый прогон: `docker compose down -v --remove-orphans`,
запуск ClickHouse и Kafka, DDL, `GEN_RUN_MODE=backfill`, batch
STG -> ODS -> DDS -> DM, инициализацию Superset и итоговую проверку
`make generated-history-check`.

Быстрая команда issue 06 проверяет путь backfill -> DM -> Superset. Стык
backfill/live, отсутствие дублей на границе и однородность визитов через
`T_end` проверены в issue 05 и не входят в быстрый штатный check issue 06.

Быстрый проверочный профиль по умолчанию:

- `GEN_SEED=4242`
- `GEN_MODEL_T0=2026-01-01T00:00:00+00:00`
- `GEN_MODEL_T_END=2026-01-01T06:00:00+00:00`
- `GEN_MODEL_TIMEZONE=UTC`
- `GEN_MODEL_TIME_SPEED=1`
- `GEN_TICK_SECONDS=60`
- `GEN_LAMBDA_BASE_PER_MIN=60`
- `GEN_JITTER_PCT=0`
- `GEN_MIN_EVENTS_PER_TICK=1`
- `GEN_MAX_EVENTS_PER_TICK=1000`

Суточный профиль остаётся доступен явно:

```bash
GEN_MODEL_T_END=2026-01-02T00:00:00+00:00 make generated-history-analytics
```

Backfill завершился:

- `events=16054`
- `visits=1516`
- `users=445`

Batch STG -> ODS -> DDS -> DM:

- STG, суммарно по 4 потокам: `64216`
- `ods.browser_event=16054`
- `ods.location_event=16054`
- `ods.device_by_click=1516`
- `ods.geo_by_click=1516`
- `dds.event=16054`
- `dds.click=1516`
- `dds.event_without_click=0`
- `dm.v_events_enriched` в `dm.dq_summary`: `16054`

Повторная команда проверки:

```bash
make generated-history-check
```

Результат ClickHouse:

- `events=16054`
- `visits=1516`
- `users=445`
- `min_event_ts=2026-01-01 00:00:00.000000`
- `max_event_ts=2026-01-01 05:59:59.353407`
- `digest=B7E183DD1835E6593CC4B33C7F5B2817`

Возвраты:

- `users=445`
- `returning_users=351`
- `returning_share=0.7887640449438202`
- `max_visits_per_user=8`

Форма длины визита:

- `visits=1516`
- `short_visit_share=0.17678100263852242`
- `median_events_per_visit=8`
- `avg_events_per_visit=10.589709762532982`
- `capped_visit_share=0.06398416886543536`
- `median_duration_sec=204`
- `p95_duration_sec=835`
- `max_events_per_visit=30`

Ordered funnel:

- `home=1387`
- `products=849`
- `cart=511`
- `payment=332`
- `confirmation=182`
- `monotonic_ok=1`
- `confirmation_share=0.13121845710165825`

Contains funnel:

- `home=1387`
- `products=1254`
- `cart=880`
- `payment=568`
- `confirmation=324`
- `monotonic_ok=1`
- `confirmation_share=0.2335976928622927`

Основные DM-витрины:

- `dm.v_events_enriched=16054`
- `dm.v_daily_traffic=797`
- `dm.v_top_pages_daily=6`
- `dm.v_utm_effectiveness=33`
- `dm.v_session_overview=1516`
- `dm.dq_summary=20`

Superset technical check:

- `superset_datasets=6`
- `superset_dashboards=1`
- `superset_dashboard_charts=10`
- `dashboard_url=http://localhost:8088/superset/dashboard/ecommerce-analytics/`
- `login_api_status=200`
- `dashboard_api_status=200`
- `dashboard_title=🛒 E-commerce Analytics Dashboard`
- зарегистрированная Superset database `clickhouse_dwh` читает
  `dm.v_events_enriched` через SQLAlchemy engine:
  `superset_clickhouse_events=16054`.

Проверки:

- `bash -n scripts/run_generated_history_analytics.sh` — ok.
- `bash -n scripts/check_generated_analytics.sh` — ok.
- `make generated-history-analytics` — ok.
- `make generated-history-check` — ok.
- `make generator-test` — 134 passed.

Документы запуска обновлены:

- `README.md`
- `docs/OPERATIONS.md`
- `generator/README.md`
- `docs/REPO_MAP.md`
- `docs/SUPERSET_DASHBOARD.md`

Follow-up:

- `.scratch/generator-model-time-startup-history/issues/07-migrate-course-from-archive-seed.md`
  — миграция учебных материалов с архивного сида на генерацию.

## Риски и что не проверено

- Ручная оценка формы dashboard глазами не выполнялась: это финальная HITL-приёмка.
- Суточный backfill не прогонялся до конца в этом срезе: он доступен через env,
  но для координатора выбран быстрый 6-часовой профиль.
- `kafka_load_dag.py` физически оставлен как архивный ручной путь; штатные
  документы больше не ведут через него как основной источник аналитики.
- `scripts/check_generated_analytics.sh` рассчитан на штатный UTC-профиль
  (`GEN_MODEL_T0/T_END` с `+00:00`). Для произвольного timezone offset в этих
  переменных нужна отдельная нормализация границ.
- Проверка `no_2022_rows` доказывает отсутствие архивного сида внутри быстрого
  чистого модельного диапазона. Она не доказывает отсутствие старых строк на
  грязном стенде вне этого диапазона; штатная команда закрывает это через
  `docker compose down -v`.

## Review gate

- Саморевью worker-а нашло слабые доказательства по проверкам issue 05 и Superset:
  добавлены возвраты, форма длины визита, ordered/contains funnel, Superset login
  API, dashboard API и чтение ClickHouse через Superset database.
- Независимый reviewer gate: `gate pass`, блокирующих находок нет.
- Minor-находка reviewer-а по старой подсказке `make data` в `scripts/run_batch.sh`
  исправлена до коммита.
- Проверки после исправлений: `make generated-history-check` — ok,
  `bash -n scripts/*.sh` — ok.
