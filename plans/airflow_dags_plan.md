# План развития Airflow DAG'ов

## Цель
Перевести оркестрацию ETL на Airflow так, чтобы пайплайн оставался устойчивым к "грязным" данным и соответствовал текущей цели проекта: Kafka → ClickHouse (STG → ODS → DDS → DM) → витрины для BI.

## Что важно учесть в текущем репозитории
- В `AGENTS.md` как quick check ожидается DAG `etl_pipeline`.
- В Airflow-контейнере сейчас нет Kafka CLI, поэтому `kafka-topics.sh` и `kafka-console-producer.sh` из `BashOperator` использовать нельзя без донастройки образа.
- DDL должен выполняться строго последовательно: `00 -> 10 -> 20 -> 30 -> 40`.
- Файл `jobs/30_dds_refresh.sql` уже включает обе загрузки (`dds.click` и `dds.event`), поэтому в MVP это одна task.

## Архитектура оркестрации
### DAG 1 (обязательный): `ddl_init`
- `schedule`: `None` (только ручной запуск).
- `catchup`: `False`.
- `max_active_runs`: `1`.
- `is_paused_upon_creation`: `True`.
- `tags`: `["ddl", "bootstrap", "clickhouse"]`.

### DAG 2 (обязательный): `etl_pipeline`
- `schedule`: `None` (ручной запуск для демо).
- `catchup`: `False`.
- `max_active_runs`: `1`.
- `tags`: `["etl", "clickhouse", "kafka", "demo"]`.

### DAG 3 (опциональный): `dq_monitor`
- `schedule`: `0 * * * *`.
- `catchup`: `False`.
- `tags`: `["dq", "monitoring"]`.

## Дизайн DAG `ddl_init`
### Params
- `verify_only`: bool, default `false` (прогон только проверок без применения DDL).

### Tasks
| Task ID | Что делает | Источник SQL/реализация |
|---------|------------|--------------------------|
| `check_clickhouse` | Проверка доступности CH (`SELECT 1`) | `PythonOperator` + `clickhouse-connect` |
| `ddl_00_databases` | Создание БД | `ddl/00_databases.sql` |
| `ddl_10_stg` | STG + Kafka Engine + MV | `ddl/10_stg.sql` |
| `ddl_20_ods` | ODS + MV STG→ODS + *_errors | `ddl/20_ods.sql` |
| `ddl_30_dds` | Таблицы DDS | `ddl/30_dds.sql` |
| `ddl_40_dm` | VIEW витрины DM | `ddl/40_dm.sql` |
| `verify_schema` | Проверка ключевых таблиц/VIEW | SQL-check |

Зависимости:
```text
check_clickhouse >> ddl_00_databases >> ddl_10_stg >> ddl_20_ods >> ddl_30_dds >> ddl_40_dm >> verify_schema
```

Примечание:
- DDL DAG запускается вручную: при первом bootstrap, при изменении схемы, после `docker compose down -v`.

## Дизайн DAG `etl_pipeline`
### Params (через Trigger DAG with config)
- `run_ingest`: bool, default `true`.
- `limit`: int, default `50`.
- `full_load`: bool, default `false`.
- `reset_topics`: bool, default `true`.
- `full_refresh`: bool, default `true`.

### TaskGroup `precheck`
| Task ID | Что делает | Реализация |
|---------|------------|------------|
| `check_clickhouse` | Проверка доступности CH (`SELECT 1`) | `PythonOperator` + `clickhouse-connect` |
| `check_schema_ready` | Проверка, что DDL уже применён (наличие `stg.browser_raw`, `ods.browser_event`, `dds.event`, `dm.v_events_enriched`) | SQL-check, fail fast |
| `check_input_files` | Проверка наличия `data/*_events.jsonl` | `PythonOperator` |

### TaskGroup `ingest` (выполняется только при `run_ingest=true`)
| Task ID | Что делает | Реализация |
|---------|------------|------------|
| `kafka_prepare_topics` | reset/create топиков по параметру `reset_topics` | `PythonOperator` + `kafka-python` AdminClient |
| `load_browser` | Публикация строк из `browser_events.jsonl` | `PythonOperator` + `KafkaProducer` |
| `load_location` | Публикация строк из `location_events.jsonl` | `PythonOperator` + `KafkaProducer` |
| `load_device` | Публикация строк из `device_events.jsonl` | `PythonOperator` + `KafkaProducer` |
| `load_geo` | Публикация строк из `geo_events.jsonl` | `PythonOperator` + `KafkaProducer` |
| `wait_for_stg_data` | Ожидание появления данных в `stg.*_raw` | `PythonSensor`/poll SQL |

Зависимости:
```text
kafka_prepare_topics >> [load_browser, load_location, load_device, load_geo] >> wait_for_stg_data
```

Примечание:
- Для демо соблюдать ограничение на малый срез данных: `limit=20..50` по умолчанию.

### TaskGroup `transform`
| Task ID | Что делает | Источник SQL |
|---------|------------|--------------|
| `wait_for_ods_data` | Ожидание строк в `ods.browser_event` | SQL-check |
| `check_ods_quality` | Базовые DQ-метрики ODS (ошибки/total) | SQL-check |
| `truncate_dds_click` | Очистка `dds.click` при `full_refresh=true` | inline SQL |
| `truncate_dds_event` | Очистка `dds.event` при `full_refresh=true` | inline SQL |
| `refresh_dds` | ODS → DDS | `jobs/30_dds_refresh.sql` |
| `check_dds_integrity` | Проверка orphan событий | inline SQL |
| `refresh_dm_summary` | DDS → DM DQ summary | `jobs/40_dm_refresh.sql` |
| `validate_dm_summary` | Проверка, что `dm.dq_summary` не пуста | SQL-check |

Зависимости:
```text
wait_for_ods_data >> check_ods_quality >> [truncate_dds_click, truncate_dds_event] >> refresh_dds >> check_dds_integrity >> refresh_dm_summary >> validate_dm_summary
```

### Итоговая цепочка `etl_pipeline`
```text
precheck >> ingest(optional) >> transform
```

## Техническая реализация (приземленно)
### ClickHouse в Airflow
- Использовать `clickhouse-connect` напрямую в Python helper, а не `SQLExecuteQueryOperator`.
- Брать параметры подключения из `conn_id = clickhouse_default` через `BaseHook.get_connection`.

### Kafka в Airflow
- Добавить зависимость `kafka-python` в `airflow/requirements.txt`.
- Использовать Python-код для:
  - reset/create топиков;
  - публикации строк из `.jsonl` (1 строка = 1 message value).

### Общие helper-функции
- `dags/utils/clickhouse_helpers.py`:
  - `execute_sql(sql: str) -> None`
  - `execute_sql_file(path: str) -> None`
  - `fetch_one(sql: str) -> tuple`
- `dags/utils/kafka_helpers.py`:
  - `prepare_topics(reset: bool) -> None`
  - `load_jsonl(file_path: str, topic: str, limit: int, full_load: bool) -> int`

## Структура файлов
```text
dags/
├── __init__.py
├── ddl_init_dag.py           # отдельный DAG для DDL (обязателен)
├── etl_pipeline_dag.py       # основной ETL DAG (обязателен)
├── dq_monitor_dag.py         # опциональный DAG мониторинга
└── utils/
    ├── __init__.py
    ├── clickhouse_helpers.py
    └── kafka_helpers.py
```

## Этапы внедрения
1. Этап 1 (MVP, обязательно):
   - Реализовать `ddl_init` и `etl_pipeline`.
   - В `etl_pipeline` оставить `precheck + transform`, ingest пока выполнять внешней командой `make data`.
   - Проверить путь: `ddl_init` -> ODS -> DDS -> DM после ручной загрузки в Kafka.
2. Этап 2:
   - Добавить в `etl_pipeline` ingest внутри DAG через `kafka-python`.
   - Добавить ветвление `run_ingest=false` для сценария "только transform".
3. Этап 3:
   - Добавить `dq_monitor` и alert callback (email/Slack/webhook).

## Критерии готовности
- В Airflow UI видны DAG `ddl_init` и `etl_pipeline`.
- `etl_pipeline` падает с понятной ошибкой, если схема не применена.
- Ручной запуск DAG с `limit=50` завершает pipeline без падений.
- После прогона:
  - в `ods.browser_event` есть строки;
  - в `dds.click` и `dds.event` есть строки;
  - `dm.dq_summary` заполнена.
- При повторном запуске с `full_refresh=true` нет неконтролируемых дублей в DDS.

## Минимальные smoke-checks
```bash
# 1) Запуск инфраструктуры
make up

# 2) Открыть Airflow UI
# http://localhost:8080 (admin/admin)

# 3) Один раз запустить ddl_init
# Trigger DAG ddl_init (без config или {"verify_only": false})

# 4) Trigger DAG etl_pipeline с config:
# {"run_ingest": true, "limit": 50, "full_load": false, "reset_topics": true, "full_refresh": true}

# 5) Проверка результатов
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM ods.browser_event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.click"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dm.dq_summary"
```

## Следующий шаг после MVP
- Для ускорения можно разделить `jobs/30_dds_refresh.sql` на два файла и распараллелить `refresh_dds_click` и `refresh_dds_event` в DAG.
- Для продакшн-режима перейти с `full_refresh` на watermark-инкремент.
