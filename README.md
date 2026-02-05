# Kaniskin docker ETL (mini clickstream demo)

Мини‑демо аналитического стека: Kafka → ClickHouse (STG → ODS → DDS → DM) + Superset + мониторинг.

## Документация

- Схема слоёв и DDL (в т.ч. Kafka → STG): `plans/clickhouse_ddl.md`
- Runbook (как поднимать/применять DDL/лить данные): `plans/runbook.md`

## Что где лежит

- Исходные данные: `data/*_events.jsonl`
- Скрипты:
  - применение DDL: `scripts/apply_clickhouse_ddl.sh`
  - загрузка данных в Kafka: `scripts/load_kafka_data.sh`

## Порты сервисов

См. `docker-compose.yml`:

- ClickHouse native: `localhost:8002`
- ClickHouse HTTP: `localhost:9123`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8082`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Superset: `http://localhost:8088`
