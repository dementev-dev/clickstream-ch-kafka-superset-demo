# Repo Map

Карта ключевых артефактов репозитория.

## Исполняемые файлы

### Airflow (ручной и учебный путь запуска)

- `airflow/dags/ddl_init_dag.py` — инициализация схемы ClickHouse
- `airflow/dags/kafka_load_dag.py` — архивная загрузка в Kafka из JSONL; не основной источник аналитики
- `airflow/dags/etl_pipeline_dag.py` — ETL процесс STG -> ODS -> DDS -> DM
- `airflow/dags/utils/kafka_helpers.py` — helper-функции для Kafka
- `airflow/dags/utils/sql_helpers.py` — чтение и подготовка SQL-файлов для DAG
- `airflow/dags/utils/airflow_params.py` — разбор и валидация параметров DAG
- `airflow/requirements.txt` — зависимости Airflow/ClickHouse plugin

### SQL

DDL (форма таблиц):

- `sql/ddl/00_databases.sql` — создание БД `stg`/`ods`/`dds`/`dm`
- `sql/ddl/stg/10_stg.sql` — STG (Kafka Engine + MV)
- `sql/ddl/ods/20_ods.sql` — ODS: типизированные таблицы и `*_errors` (наполняются batch, не MV)
- `sql/ddl/dds/30_dds.sql` — DDS (таблицы для batch-загрузки)
- `sql/ddl/dm/40_dm.sql` — DM (витрины VIEW)

Трансформации (наполнение, шаги `etl_pipeline`):

- `sql/ods/20_stg_to_ods.sql` — STG -> ODS: типизация + DQ-split (валидный ключ → `ods.*`, любая ошибка → `ods.*_errors`)
- `sql/dds/30_ods_to_dds.sql` — ODS -> DDS (argMax + LEFT JOIN)
- `sql/dm/40_dds_to_dm.sql` — DDS -> DM: пересборка `dm.dq_summary` (TRUNCATE+INSERT) по всем слоям; сами витрины `dm.v_*` — это VIEW из DDL

### Superset

- `superset/init_superset.py` — подключение к ClickHouse + создание датасетов
- `superset/create_dashboard.py` — сборка дашборда с чартами

### Скрипты

Shell-скрипты `scripts/*` и Makefile-обёртки дают повторяемый локальный запуск.
Основной чистый путь аналитики — `make generated-history-analytics`.

- `scripts/apply_clickhouse_ddl.sh` — применение DDL
- `scripts/load_kafka_data.sh` — архивная загрузка `data/*.jsonl` в Kafka
- `scripts/run_batch.sh` — batch-процесс
- `scripts/run_generated_history_analytics.sh` — чистый прогон стартовой истории до DM и Superset
- `scripts/check_generated_analytics.sh` — проверка DM-витрин и Superset metadata на данных генерации

## Данные и конфиги

- `data/*.jsonl` — архивная фактура для генератора; не основной источник аналитики
- `configs/` — конфиги ClickHouse, Prometheus, Grafana
- `configs/prometheus.yml` — конфигурация Prometheus (scrape targets для ClickHouse, Kafka, Airflow)
- `configs/statsd_mapping.yml` — маппинг StatsD → Prometheus метрик для Airflow
- `configs/default_user.xml` — пользователь ClickHouse (default/123456)
- `configs/prometheus_ch.xml` — встроенный Prometheus endpoint ClickHouse
- `configs/grafana/provisioning/alerting/clickhouse-alert-rules.yml` — правила алертинга Grafana для ClickHouse
- `configs/grafana/provisioning/alerting/kafka-alert-rules.yml` — правила алертинга Grafana для Kafka
- `configs/grafana/provisioning/alerting/airflow-alert-rules.yml` — правила алертинга Grafana для Airflow
- `configs/grafana/provisioning/dashboards/clickhouse-overview.json` — дашборд ClickHouse
- `configs/grafana/provisioning/dashboards/kafka-overview.json` — дашборд Kafka
- `configs/grafana/provisioning/dashboards/airflow-overview.json` — дашборд Airflow

## Документация

- `README.md` — быстрый старт и обзор проекта
- `docs/ARCHITECTURE.md` — техническая архитектура
- `docs/OPERATIONS.md` — запуск, проверки, troubleshooting
- `docs/SUPERSET_DASHBOARD.md` — настройка и использование дашборда Superset
- `docs/DE-task.md` — исходное задание
- `docs/COMMIT_RULES.md` — правила коммитов
- `docs/course/` — продвинутый учебный курс на базе стенда (PRD, план, уроки)
- `docs/adr/` — архитектурные решения (ADR)
- `docs/agents/` — контракты для агентских скиллов (issue-tracker, triage, domain)

## Legacy-планы

- `plans/clickhouse_ddl.md` — исходный план (inline DDL)
- `plans/runbook.md` — ранний runbook
- `plans/kafka_ingest_plan.md` — ранний план Kafka ingest
- `plans/monitoring_airflow_plan.md` — план подключения Airflow мониторинга
- `plans/monitoring_kafka_plan.md` — план подключения Kafka мониторинга
