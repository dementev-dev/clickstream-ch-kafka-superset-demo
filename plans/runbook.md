# Runbook: Airflow-first запуск демо

Документ фиксирует канонический пользовательский сценарий через Airflow DAG'и.
Внутренности слоёв ClickHouse описаны в `plans/clickhouse_ddl.md` и `docs/ARCHITECTURE.md`.

## Предпосылки

- Docker + Docker Compose.
- Доступ к Docker daemon.

## Канонический сценарий (через Airflow)

1) Поднять инфраструктуру:

```bash
make up
docker compose ps
```

2) Инициализировать схему ClickHouse:

```bash
docker compose exec -T airflow-webserver airflow dags trigger ddl_init
```

3) Загрузить данные в Kafka через DAG `kafka_load`:

```bash
# Рекомендуется для демо: небольшой срез
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 50, "reset_topics": true}'

# Полная загрузка (limit=0 по умолчанию)
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"reset_topics": true}'
```

4) Запустить ETL:

```bash
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline \
  --conf '{"full_refresh": true}'
```

5) Проверить результат:

```bash
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT count() FROM stg.browser_raw"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT count() FROM ods.browser_event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT count() FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name"
```

## Параметры DAG'ов

- `ddl_init`:
  - `verify_only` (bool, default: `false`) — только проверка схемы, без применения DDL.
- `kafka_load`:
  - `limit` (int, default: `0`) — количество строк на поток (`0` = весь файл).
  - `reset_topics` (bool, default: `true`) — удалить и создать топики заново.
- `etl_pipeline`:
  - `full_refresh` (bool, default: `true`) — очищать DDS перед загрузкой.
  - `wait_stg_timeout_sec` (int, default: `600`) — таймаут ожидания данных в STG.

## Роль Make-таргетов

- `make up` — основной способ поднять стек.
- `make ddl`, `make transform` — технический fallback для низкоуровневой диагностики вне Airflow.
- Загрузка в Kafka в runbook выполняется только через DAG `kafka_load`.

## Важные примечания

- Для связей контейнеров используйте `kafka:29092` (не `localhost:9092`).
- После `docker compose down -v` нужно заново выполнить:
  1. `ddl_init`
  2. `kafka_load`
  3. `etl_pipeline`

## Связанные документы

- План ingest: `plans/kafka_ingest_plan.md`
- План DAG'ов: `plans/airflow_dags_plan.md`
- Архитектура: `docs/ARCHITECTURE.md`
