# Аудит точности технической документации против кода

Дата: 2026-06-06 · Статус: **закрыто** (все 12 находок починены 2026-06-06)

> Резолюция. 🔴 #1–#3 правлены в `ARCHITECTURE.md` (схема `*_errors`, DQ-split как
> пересекающийся, `dds.event` как browser-driven LEFT JOIN + маркер `location_not_found`) —
> формулировки подтянуты к урокам 2–3. #4: владелец решил **убрать** `make superset-export`
> (раздел доки + target в Makefile + битый `superset/export_dashboard.py` удалены).
> 🟡 #6–#10 и 🟢 #11–#12 закрыты в `ARCHITECTURE.md`/`OPERATIONS.md`/`REPO_MAP.md`
> (добавлен перечень DQ-маркеров, параметр `wait_stg_timeout_sec`, порты Superset/креды CH,
> пропущенные артефакты). Заодно зафиксирована рамка: основной путь — Airflow,
> `scripts/`/`make` — запасной.

Контекст: при переработке корневого `README.md` (mentee-first) встал вопрос, можно ли
смело отправлять читателя в профильные доки — не устарели ли они сами. Прогнали сверку
каждого профильного дока против кода (источник истины — SQL/DDL, batch-SQL, DAG-и,
`docker-compose.yml`, `Makefile`, скрипты Superset). Ниже — подтверждённые расхождения.

README по итогам признан самостоятельно корректным (он ссылается на доки, а не повторяет
их детали) и закоммичен отдельно. Починку доков решили делать отдельным проходом, с
оглядкой на параллельную работу Codex (особенно по ARCHITECTURE).

## 🔴 Вводит в заблуждение / сломано

1. **ARCHITECTURE: схема `ods.*_errors` описана неверно.** Док подаёт таблицы ошибок как
   копию типизированной таблицы события (с `event_id`, `parse_errors`). Реально это
   метаданные Kafka + `raw` + `error_reason` — другая схема.
   `docs/ARCHITECTURE.md:149,529-532` против `sql/ddl/ods/20_ods.sql:46-58`.

2. **ARCHITECTURE: DQ-split подан как взаимоисключающий.** Док: «валидные → основная
   таблица, ошибки → errors». Реально строка с валидным `event_id`, но битым
   `event_ts`/`click_id` попадает **в обе** таблицы одновременно.
   `docs/ARCHITECTURE.md:160-162,520-521` против `sql/ods/20_stg_to_ods.sql:67,91-96`.

3. **ARCHITECTURE: `dds.event` — browser-driven LEFT JOIN, а не симметричный JOIN 1:1.**
   event_id, которые есть только в `location_event` (без browser), в `dds.event` не
   попадают. Маркер `location_not_found` в доке не упомянут.
   `docs/ARCHITECTURE.md:205,446-460,398` против `sql/dds/30_ods_to_dds.sql:139,141-172`.

4. **SUPERSET_DASHBOARD: `make superset-export` задокументирован как рабочий, но сломан.**
   `superset/export_dashboard.py:22-23` импортирует несуществующие модули Superset
   (`superset.dashboards.data_access_layer`, `superset.charts.data_access_layer`) и падает
   в `except ImportError → sys.exit(1)`. Плюс реальный файл экспорта —
   `superset/dashboards/ecommerce_analytics.zip.json`, а док обещает `ecommerce_analytics.json`.
   `docs/SUPERSET_DASHBOARD.md:81-83,202-207`.

5. **REPO_MAP: пропущен `sql/ods/20_stg_to_ods.sql`** — первый шаг ETL (STG→ODS) вообще
   отсутствует в карте исполняемых артефактов. `docs/REPO_MAP.md:21-23`.

## 🟡 Неполно / неточно

6. **ARCHITECTURE: `v_session_overview` считает только авторизованных**
   (`WHERE user_domain_id IS NOT NULL`) — в доке не сказано.
   `docs/ARCHITECTURE.md:304` против `sql/ddl/dm/40_dm.sql:148`.

7. **ARCHITECTURE: `dq_summary` шире, чем сказано.** Док: «слои stg/ods/dds». Реально есть
   ещё слой `dm` (`v_events_enriched`) и метрика `orphan_events`.
   `docs/ARCHITECTURE.md:320-331` против `sql/dm/40_dds_to_dm.sql:99-115`.

8. **ARCHITECTURE: перечень DQ-маркеров неполон.** Не упомянуты `geo_country_missing`
   (DDS click), `bad_geo_latitude`/`bad_geo_longitude`/`bad_user_domain_id` (ODS),
   `os_timezone` в ER-диаграмме DEVICE_BY_CLICK.
   `sql/dds/30_ods_to_dds.sql:50`, `sql/ods/20_stg_to_ods.sql:172,227-228`.

9. **OPERATIONS: у `etl_pipeline` не задокументирован параметр `wait_stg_timeout_sec`**
   (default 600). `docs/OPERATIONS.md:57-66` против `airflow/dags/etl_pipeline_dag.py:265-268`.

10. **REPO_MAP: пропущены артефакты.** Скрипты `superset/{init_superset,create_dashboard,export_dashboard}.py`,
    утилиты `airflow/dags/utils/{sql_helpers,airflow_params}.py`. Описание
    `sql/dm/40_dds_to_dm.sql` занижено («обновление dq_summary» вместо полного
    TRUNCATE+INSERT DDS→DM). `docs/REPO_MAP.md:9-13,21-23`.

## 🟢 Мелочи

11. **OPERATIONS:** Superset (`8088`) и креды ClickHouse (`default/123456`) не вынесены в
    каноническую секцию портов/доступа (фигурируют только ниже по тексту).
12. **REPO_MAP:** список документации неполон (нет `SUPERSET_DASHBOARD.md`, `docs/course/`,
    `docs/adr/`, демо-скриптов и т. п.).

## Вывод для владельца

Концептуальные расхождения ARCHITECTURE (DQ-split, сборка DDS с «сиротами») — это ровно
то, что подробнее и точнее разбирают уроки 2–3 курса. ARCHITECTURE.md отстал от
реализации, а свежие уроки догнали код. Де-факто каноном по этим темам стали уроки, а не
ARCHITECTURE — это стоит учесть при починке (возможно, ARCHITECTURE проще подтянуть к
формулировкам уроков, чем переписывать с нуля).

## Предлагаемый порядок починки

1. Сначала 🔴 (искажают модель данных и ломают команду).
2. Перед правкой ARCHITECTURE — свериться, не редактирует ли её Codex.
3. `make superset-export`: решить — чинить импорты `export_dashboard.py` или убрать команду
   из документации, если экспорт больше не используется.
