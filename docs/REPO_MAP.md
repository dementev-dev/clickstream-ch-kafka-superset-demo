# Repo Map

Карта ключевых артефактов репозитория.

## Исполняемые файлы

### Airflow

- `airflow/dags/ddl_init_dag.py` — инициализация схемы ClickHouse
- `airflow/dags/kafka_load_dag.py` — загрузка в Kafka из JSONL
- `airflow/dags/etl_pipeline_dag.py` — ETL процесс STG -> ODS -> DDS -> DM
- `airflow/dags/utils/kafka_helpers.py` — helper-функции для Kafka
- `airflow/requirements.txt` — зависимости Airflow/ClickHouse plugin

### SQL

- `sql/ddl/00_databases.sql` — создание БД `stg`/`ods`/`dds`/`dm`
- `sql/ddl/stg/10_stg.sql` — STG (Kafka Engine + MV)
- `sql/ddl/ods/20_ods.sql` — ODS (типизация + MV для ошибок)
- `sql/ddl/dds/30_dds.sql` — DDS (таблицы для batch-загрузки)
- `sql/ddl/dm/40_dm.sql` — DM (витрины VIEW)
- `sql/dds/30_ods_to_dds.sql` — ODS -> DDS (argMax + JOIN)
- `sql/dm/40_dds_to_dm.sql` — обновление `dq_summary`

### Скрипты

- `scripts/apply_clickhouse_ddl.sh` — применение DDL
- `scripts/load_kafka_data.sh` — загрузка в Kafka
- `scripts/run_batch.sh` — batch-процесс

## Данные и конфиги

- `data/*.jsonl` — исходные данные (могут быть грязными)
- `configs/` — конфиги ClickHouse, Prometheus, Grafana
- `configs/grafana/provisioning/alerting/clickhouse-alert-rules.yml` — правила алертинга Grafana для ClickHouse

## Документация

- `README.md` — быстрый старт и обзор проекта
- `docs/ARCHITECTURE.md` — техническая архитектура
- `docs/OPERATIONS.md` — запуск, проверки, troubleshooting
- `docs/DE-task.md` — исходное задание
- `docs/COMMIT_RULES.md` — правила коммитов

## Legacy-планы

- `plans/clickhouse_ddl.md` — исходный план (inline DDL)
- `plans/runbook.md` — ранний runbook
- `plans/kafka_ingest_plan.md` — ранний план Kafka ingest
