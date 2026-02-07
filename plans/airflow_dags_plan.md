# План развития Airflow DAG'ов

## Цель
Перевести оркестрацию ETL на Airflow так, чтобы пайплайн оставался устойчивым к "грязным" данным и соответствовал целям проекта: Kafka → ClickHouse (STG → ODS → DDS → DM) → витрины для BI.

## Что важно учесть в текущем репозитории
- В `AGENTS.md` как quick check ожидается DAG `etl_pipeline`.
- В Airflow-контейнере сейчас нет Kafka CLI, поэтому `kafka-topics.sh` и `kafka-console-producer.sh` из `BashOperator` не используем.
- DDL должен выполняться строго последовательно: `00 -> 10 -> 20 -> 30 -> 40`.
- Файл `jobs/30_dds_refresh.sql` уже включает обе загрузки (`dds.click` и `dds.event`), поэтому в MVP это одна task.

## Архитектура оркестрации
### DAG 1 (обязательный): `ddl_init`
- `schedule`: `None` (только ручной запуск).
- `catchup`: `False`.
- `max_active_runs`: `1`.
- `is_paused_upon_creation`: `True`.
- `tags`: `["ddl", "bootstrap", "clickhouse"]`.

### DAG 2 (обязательный): `kafka_load`
- `schedule`: `None` (ручной/экспериментальный запуск).
- `catchup`: `False`.
- `max_active_runs`: `1`.
- `is_paused_upon_creation`: `True`.
- `tags`: `["kafka", "ingest", "experiments"]`.
- Реализация: этап 2 (после MVP).

### DAG 3 (обязательный): `etl_pipeline`
- `schedule`: `None` (ручной запуск для демо).
- `catchup`: `False`.
- `max_active_runs`: `1`.
- `tags`: `["etl", "clickhouse", "demo"]`.

### DAG 4 (опциональный): `dq_monitor`
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
- `ddl_init` запускается вручную: при первом bootstrap, после `docker compose down -v`, после изменений схемы.

## Дизайн DAG `kafka_load` (отдельный независимый контур)
### Params (через Trigger DAG with config)
- `limit`: int, default `50`.
- `full_load`: bool, default `false`.
- `reset_topics`: bool, default `true`.
- `load_browser`: bool, default `true`.
- `load_location`: bool, default `true`.
- `load_device`: bool, default `true`.
- `load_geo`: bool, default `true`.

### TaskGroup `precheck`
| Task ID | Что делает | Реализация |
|---------|------------|------------|
| `check_kafka` | Проверка доступности Kafka broker | `PythonOperator` + `kafka-python` |
| `check_input_files` | Проверка наличия `data/*_events.jsonl` | `PythonOperator` |
| `validate_load_params` | Валидация параметров загрузки (`limit`, `full_load`, флаги потоков) | `PythonOperator` |

### TaskGroup `ingest`
| Task ID | Что делает | Реализация |
|---------|------------|------------|
| `prepare_topics` | reset/create топиков по `reset_topics` | `PythonOperator` + AdminClient |
| `load_browser_events` | Публикация `browser_events.jsonl` | `PythonOperator` + KafkaProducer |
| `load_location_events` | Публикация `location_events.jsonl` | `PythonOperator` + KafkaProducer |
| `load_device_events` | Публикация `device_events.jsonl` | `PythonOperator` + KafkaProducer |
| `load_geo_events` | Публикация `geo_events.jsonl` | `PythonOperator` + KafkaProducer |
| `verify_publish_counts` | Проверка, что отправлено > 0 сообщений в выбранные потоки | `PythonOperator` (по XCom) |

Зависимости:
```text
precheck >> prepare_topics >> [load_browser_events, load_location_events, load_device_events, load_geo_events] >> verify_publish_counts
```

Примечания:
- DAG намеренно независим от `etl_pipeline`: можно запускать ingest отдельно для экспериментов.
- Авто-триггер `etl_pipeline` не включаем по умолчанию; при необходимости добавляется отдельным параметром позже.

## Дизайн DAG `etl_pipeline`
### Params (через Trigger DAG with config)
- `full_refresh`: bool, default `true`.
- `wait_ods_timeout_sec`: int, default `600`.

### TaskGroup `precheck`
| Task ID | Что делает | Реализация |
|---------|------------|------------|
| `check_clickhouse` | Проверка доступности CH (`SELECT 1`) | `PythonOperator` + `clickhouse-connect` |
| `check_schema_ready` | Проверка, что DDL уже применён (`stg.browser_raw`, `ods.browser_event`, `dds.event`, `dm.v_events_enriched`) | SQL-check, fail fast |

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
precheck >> transform
```

## Взаимодействие DAG'ов
Базовый сценарий:
```text
ddl_init -> kafka_load -> etl_pipeline
```

Экспериментальные сценарии:
- `kafka_load` отдельно: проверить разные наборы/параметры загрузки без запуска transform.
- `etl_pipeline` отдельно: повторно пересчитать DDS/DM по уже загруженным данным.

## Техническая реализация (приземленно)
### ClickHouse в Airflow
- Использовать `clickhouse-connect` напрямую в Python helper.
- Брать параметры подключения из `conn_id = clickhouse_default` через `BaseHook.get_connection`.

### Kafka в Airflow
- Добавить `kafka-python` в `airflow/requirements.txt`.
- Использовать Python-код для:
  - reset/create топиков;
  - публикации строк из `.jsonl` (`1 строка = 1 message value`).

### Общие helper-функции
- `dags/utils/clickhouse_helpers.py`:
  - `execute_sql(sql: str) -> None`
  - `execute_sql_file(path: str) -> None`
  - `fetch_one(sql: str) -> tuple`
- `dags/utils/kafka_helpers.py`:
  - `prepare_topics(reset: bool) -> None`
  - `load_jsonl(file_path: str, topic: str, limit: int, full_load: bool) -> int`
  - `check_kafka_ready() -> None`

## Структура файлов
```text
dags/
├── __init__.py
├── ddl_init_dag.py           # отдельный DAG для DDL (обязателен)
├── kafka_load_dag.py         # отдельный DAG для ingest в Kafka (обязателен)
├── etl_pipeline_dag.py       # основной DAG ODS -> DDS -> DM (обязателен)
├── dq_monitor_dag.py         # опциональный DAG мониторинга
└── utils/
    ├── __init__.py
    ├── clickhouse_helpers.py
    └── kafka_helpers.py
```

## Этапы внедрения
1. Этап 1 (MVP, обязательно):
   - Реализовать `ddl_init` и `etl_pipeline`.
   - Для загрузки данных использовать существующий сценарий `make data`.
   - Проверить путь `ddl_init -> make data -> etl_pipeline`.
2. Этап 2:
   - Реализовать отдельный DAG `kafka_load` на `kafka-python`.
   - Перенести загрузку из `make data` в `kafka_load` (функциональный паритет).
   - Добавить в `kafka_load` расширенные параметры экспериментов (выбор потоков, сценарии reset/no-reset).
   - Добавить опциональный параметр автотриггера `etl_pipeline` (по умолчанию `false`).
3. Этап 3:
   - Добавить `dq_monitor` и alert callback (email/Slack/webhook).

## Критерии готовности
- Этап 1:
  - В Airflow UI видны DAG `ddl_init` и `etl_pipeline`.
  - `etl_pipeline` падает с понятной ошибкой, если схема не применена или ODS пуста.
  - После прогона `make data -> etl_pipeline`:
    - в `ods.browser_event` есть строки;
    - в `dds.click` и `dds.event` есть строки;
    - `dm.dq_summary` заполнена.
- Этап 2:
  - В Airflow UI дополнительно виден DAG `kafka_load`.
  - `kafka_load` с `limit=50` завершает отправку сообщений без падений.
  - После прогона `kafka_load -> etl_pipeline` результаты совпадают с `make data -> etl_pipeline`.
- Для всех этапов:
  - При повторном запуске `etl_pipeline` с `full_refresh=true` нет неконтролируемых дублей в DDS.

## Минимальные smoke-checks
```bash
# 1) Запуск инфраструктуры
make up

# 2) Открыть Airflow UI
# http://localhost:8080 (admin/admin)

# 3) Один раз запустить ddl_init
# Trigger DAG ddl_init (без config или {"verify_only": false})

# 4) Этап 1: загрузить данные текущим способом
make data

# 5) Запустить etl_pipeline
# {"full_refresh": true}

# 6) Проверка результатов
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM ods.browser_event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.click"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dm.dq_summary"

# 7) Этап 2: после реализации kafka_load
# Trigger DAG kafka_load {"limit": 50, "full_load": false, "reset_topics": true}
# Trigger DAG etl_pipeline {"full_refresh": true}
```

## Следующий шаг после MVP
- Разделить `jobs/30_dds_refresh.sql` на два файла и распараллелить `refresh_dds_click` и `refresh_dds_event`.
- Перейти с `full_refresh` на watermark-инкремент.
