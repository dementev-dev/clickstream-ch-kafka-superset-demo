# Operations Runbook

Операционный runbook для локального запуска и проверки пайплайна.

## Локальный запуск

Базовые команды:

- `make up` (или `docker compose up -d`)
- `make ddl` (применяет SQL из `sql/ddl/00_databases.sql` и `sql/ddl/*/*.sql` в ClickHouse)
- `make data` (пересоздаёт топики и заливает небольшой срез данных в Kafka; полный режим — `FULL=1 make data`)
- `make transform` (запускает batch-процесс ODS -> DDS -> DM)
- `docker compose ps`
- `docker compose logs -f --tail=200 <service>`
- `docker compose down` (сохраняет named volumes, включая `clickhouse-data`)
- `docker compose down -v` (удаляет named volumes, использовать осознанно)

## Порты

Порты задаются в `docker-compose.yml`:

- ClickHouse native: `localhost:8002`
- ClickHouse HTTP: `localhost:9123`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8082`
- Airflow: `http://localhost:8080` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Airflow DAGs

### `ddl_init`

- Запуск: ручной (`Trigger DAG`)
- Параметр: `verify_only` (`bool`, default `false`)
- Назначение: создаёт БД и таблицы в ClickHouse от `00_databases` до `40_dm`

### `kafka_load`

- Запуск: ручной (`Trigger DAG with config`)
- Параметры:
  - `limit` (`int`, default `0`) — количество строк (`0` = все)
  - `reset_topics` (`bool`, default `true`) — пересоздать топики
- Примеры:

```json
{}
```

```json
{"limit": 100}
```

### `etl_pipeline`

- Запуск: ручной (`Trigger DAG with config`)
- Параметр: `full_refresh` (`bool`, default `true`) — очистить DDS перед загрузкой
- Зависимость: требует наличия данных в STG (от `kafka_load` или `make data`)

## Рекомендуемый сценарий (фаза 2)

```bash
# 1. Запуск инфраструктуры
make up

# 2. Инициализация схемы (один раз)
# Airflow UI -> DAGs -> ddl_init -> Trigger DAG

# 3. Загрузка данных через Airflow
# Airflow UI -> DAGs -> kafka_load -> Trigger DAG with config
# Параметры по умолчанию: limit=0, reset_topics=true

# 4. Запуск ETL
# Airflow UI -> DAGs -> etl_pipeline -> Trigger DAG with config
# {"full_refresh": true}

# 5. Проверка результатов
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM ods.browser_event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT * FROM dm.dq_summary"
```

## Быстрые проверки

- Kafka ingest: наличие данных в `stg.*` и типизированных строк в `ods.*`.
- Мониторинг: доступность `/metrics` у ClickHouse и скрейп в Prometheus.
- Airflow UI: `http://localhost:8080` показывает DAG `ddl_init`, `kafka_load`, `etl_pipeline`.
- BI: витрина `dm.v_events_enriched` отвечает за разумное время при фильтре по дате.

## Troubleshooting

- `etl_pipeline` падает с ошибкой схемы: сначала запустить `ddl_init`.
- После `docker compose down -v` нужно повторно прогнать: `ddl_init` -> `kafka_load` -> `etl_pipeline`.
- Для демо по умолчанию использовать малый срез данных; полный прогон делать осознанно.
