# План тестирования стенда (Smoke + Full)

Документ описывает два контура проверки системы:
- быстрый `smoke` для регрессий после изменений;
- полный `full` для финальной валидации end-to-end.

## Цель

Проверить, что стек `Kafka + ClickHouse + Airflow + Superset + Prometheus/Grafana`:
- стабильно поднимается;
- загружает данные по пути `kafka_load -> STG -> etl_pipeline -> ODS/DDS/DM`;
- корректно обрабатывает «грязные» записи (ошибки фиксируются в ODS, пайплайн не падает);
- отдает метрики и дашборды мониторинга.

## Общие принципы

- По умолчанию используем малый срез (`limit=50`) для быстрых и повторяемых проверок.
- Полный прогон (`limit=0`) выполняем отдельно как long-run сценарий.
- Основной путь запуска — через Airflow DAG.
- Критерий успеха: не только `Success` DAG, но и проверки данных/ошибок/мониторинга.

---

## Контур A: Smoke (быстрый)

Ожидаемая длительность: ~10-20 минут.

### A.1 Подготовка окружения

```bash
# Полная очистка стенда
make clean

# Запуск инфраструктуры
make up

# Проверка контейнеров
docker compose ps
```

Ожидаем:
- `airflow-init` в `Exited (0)`;
- остальные сервисы в `Up` (включая `superset`, `prometheus`, `grafana`, `kafka-exporter`, `statsd-exporter`).

### A.2 DDL и минимальная загрузка данных

```bash
# Инициализация схемы
docker compose exec -T airflow-webserver airflow dags trigger ddl_init

# Быстрый ingest: по 50 строк на поток
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 50, "reset_topics": true}'
```

Проверки:

```bash
# STG не пустой

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "
SELECT 'browser_raw' AS table, count() AS cnt FROM stg.browser_raw
UNION ALL
SELECT 'location_raw', count() FROM stg.location_raw
UNION ALL
SELECT 'device_raw', count() FROM stg.device_raw
UNION ALL
SELECT 'geo_raw', count() FROM stg.geo_raw
"
```

Ожидаем: во всех 4 таблицах `cnt > 0`.

### A.3 ETL и проверки слоев

```bash
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline \
  --conf '{"full_refresh": true}'
```

Проверки:

```bash
# ODS / DDS / DM
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "
SELECT 'ods.browser_event' AS table, count() AS cnt FROM ods.browser_event
UNION ALL
SELECT 'dds.event', count() FROM dds.event
UNION ALL
SELECT 'dds.click', count() FROM dds.click
UNION ALL
SELECT 'dm.dq_summary', count() FROM dm.dq_summary
"

# Базовая целостность DDS (ожидаем 0 orphan-событий)
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "
SELECT countIf(click_id IS NOT NULL AND click_id NOT IN (SELECT click_id FROM dds.click)) AS orphan_events
FROM dds.event
"
```

Ожидаем:
- `ods.browser_event`, `dds.event`, `dm.dq_summary` > 0;
- `orphan_events = 0`.

### A.4 Smoke мониторинга

```bash
# Prometheus targets
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'

# Grafana health
curl -s -u admin:admin http://localhost:3000/api/health

# Дашборды по UID
curl -s -u admin:admin "http://localhost:3000/api/dashboards/uid/clickhouse-overview" | grep -o '"title":"[^"]*"'
curl -s -u admin:admin "http://localhost:3000/api/dashboards/uid/kafka-overview" | grep -o '"title":"[^"]*"'
curl -s -u admin:admin "http://localhost:3000/api/dashboards/uid/airflow-overview" | grep -o '"title":"[^"]*"'
```

Критерий успеха smoke:
- сервисы подняты;
- 3 DAG (`ddl_init`, `kafka_load`, `etl_pipeline`) успешны;
- данные проходят до DM;
- мониторинг и дашборды доступны.

---

## Контур B: Full (полный)

Ожидаемая длительность: ~30-60 минут.

### B.1 Полная загрузка и полный ETL

```bash
# Чистый старт
make clean && make up

# DDL
docker compose exec -T airflow-webserver airflow dags trigger ddl_init

# Полный ingest (все строки)
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 0, "reset_topics": true}'

# Полный ETL
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline \
  --conf '{"full_refresh": true}'
```

### B.2 Проверка объемов и DQ

```bash
# Фактические размеры входа
wc -l data/*.jsonl

# Сводка по слоям
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "
SELECT 'STG' as layer, sum(rows) as total_rows FROM (
    SELECT count() as rows FROM stg.browser_raw UNION ALL
    SELECT count() FROM stg.location_raw UNION ALL
    SELECT count() FROM stg.device_raw UNION ALL
    SELECT count() FROM stg.geo_raw
) UNION ALL
SELECT 'ODS', sum(rows) FROM (
    SELECT count() FROM ods.browser_event UNION ALL
    SELECT count() FROM ods.location_event UNION ALL
    SELECT count() FROM ods.device_by_click UNION ALL
    SELECT count() FROM ods.geo_by_click
) UNION ALL
SELECT 'DDS', sum(rows) FROM (
    SELECT count() FROM dds.event UNION ALL
    SELECT count() FROM dds.click
)"

# DQ summary

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name"
```

Критерии успеха full:
- STG заполнен по всем 4 потокам;
- ODS/DDS/DM заполнены;
- `dm.dq_summary` не пуста и содержит метрики всех слоев.

### B.3 Тест восстановления stop/start

```bash
# Остановить без удаления данных
docker compose stop

# Проверить volumes

docker volume ls | grep -E 'clickhouse-data|kafka-data|pgmeta|grafana_lib|superset_data|superset_config'

# Поднять обратно
docker compose start

# Проверить, что данные сохранились

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT count() FROM dds.event"
```

---

## Сценарий C: «Грязные» данные не валят пайплайн

Цель: доказать, что невалидные записи фиксируются в `ods.*_errors`, а ETL завершается успешно.

Предусловие: выполнен `Контур A` или `Контур B` (в STG уже есть валидные данные).

### C.1 Инъекция невалидных записей в STG

```bash
# browser: невалидные UUID и timestamp

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "INSERT INTO stg.browser_raw (raw) VALUES ('{\"event_id\":\"bad-uuid\",\"event_timestamp\":\"bad-ts\",\"click_id\":\"bad-click\",\"event_type\":\"pageview\"}')"

# location: невалидный event_id

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "INSERT INTO stg.location_raw (raw) VALUES ('{\"event_id\":\"bad-uuid\",\"page_url\":\"https://example.com\"}')"

# device: невалидные click_id и user_domain_id

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "INSERT INTO stg.device_raw (raw) VALUES ('{\"click_id\":\"bad-uuid\",\"user_domain_id\":\"bad-uuid\",\"device_type\":\"Mobile\"}')"

# geo: невалидные click_id и координаты

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "INSERT INTO stg.geo_raw (raw) VALUES ('{\"click_id\":\"bad-uuid\",\"geo_latitude\":\"abc\",\"geo_longitude\":\"def\"}')"
```

### C.2 Повторный ETL

```bash
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline \
  --conf '{"full_refresh": true}'
```

Ожидаем: DAG `etl_pipeline` завершен в `Success`.

### C.3 Ассерты по ошибкам

```bash
# Ошибки должны попасть в *_errors таблицы

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "
SELECT 'browser_event_errors' AS table, count() AS cnt FROM ods.browser_event_errors
UNION ALL
SELECT 'location_event_errors', count() FROM ods.location_event_errors
UNION ALL
SELECT 'device_by_click_errors', count() FROM ods.device_by_click_errors
UNION ALL
SELECT 'geo_by_click_errors', count() FROM ods.geo_by_click_errors
"

# При этом пайплайн продолжает давать бизнес-слой

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 \
  --query "SELECT count() FROM dds.event"
```

Критерии успеха сценария C:
- `etl_pipeline` не падает;
- хотя бы одна `*_errors` таблица увеличилась;
- `dds.event` остается непустой (валидные данные продолжают обрабатываться).

---

## Проверка алертов (фактический набор)

Проверяем, что в Grafana загружены именно текущие provisioned-правила:

```bash
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | grep -o '"title":"[^"]*"'
```

Ожидаемые правила:

- ClickHouse:
  - `ClickHouse Failed Queries Rate`
  - `ClickHouse Memory Resident High`
  - `ClickHouse Parts Active High`
- Airflow:
  - `Airflow Scheduler Down`
  - `Airflow Queue Backlog`
  - `High Task Failure Rate`
  - `High DAG Parse Time`
- Kafka:
  - `Kafka Broker Down`
  - `Kafka Consumer Lag High`
  - `Kafka No Messages Produced`

---

## Финальный чек-лист приемки

- `Smoke` проходит стабильно после изменений в коде.
- `Full` проходит перед демонстрацией/релизом.
- Сценарий `Грязные данные` подтверждает, что ошибки локализуются в ODS и не ломают ETL.
- Метрики и дашборды доступны, алерты совпадают с текущей конфигурацией provisioning.
