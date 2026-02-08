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
- Airflow UI: `http://localhost:8080` показывает DAG `ddl_init`, `kafka_load`, `etl_pipeline`.
- BI: витрина `dm.v_events_enriched` отвечает за разумное время при фильтре по дате.

---

## Мониторинг

### Prometheus + Grafana для ClickHouse

Стек мониторинга поднимается вместе с остальной инфраструктурой:

```bash
# Проверить статус сервисов мониторинга
docker compose ps prometheus grafana

# Проверить скрейп ClickHouse в Prometheus
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

### Конфигурация

- **Prometheus** (`configs/prometheus.yml`): скрейп ClickHouse на порту `9126/metrics`
- **ClickHouse** (`configs/prometheus_ch.xml`): включён экспорт метрик в формате Prometheus
- **Grafana provisioning** (`configs/grafana/provisioning/`):
  - Datasource Prometheus автоматически настроен
  - Dashboard "ClickHouse Overview" загружается при старте
  - Alert rules для ClickHouse загружаются при старте

### Дашборд ClickHouse Overview

URL: `http://localhost:3000/d/clickhouse-overview/clickhouse-overview`

| Раздел | Метрики |
|--------|---------|
| System Health | CPU Usage, Memory Resident, Memory Code |
| Query Performance | Queries/sec, Active Queries, Failed Queries, Total Queries, Inserted Rows/sec |
| MergeTree Storage | Total Parts, Parts by State, Total Merges, Merges/sec |

Принятое решение по метрикам: сверили naming через Context7 (`/clickhouse/clickhouse-docs`, раздел Prometheus interface) и заменили недоступные в `25.1` серии на фактически экспортируемые (`ClickHouseProfileEvents_InsertedRows`, `ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables`, `ClickHouseMetrics_Parts*`).

### Проверка метрик

```bash
# Проверить, что Prometheus собирает метрики
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseAsyncMetrics_MemoryResident"

# Проверить счётчик запросов
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseProfileEvents_Query"
```

### Алерты Grafana

Provisioning-файл: `configs/grafana/provisioning/alerting/clickhouse-alert-rules.yml`

Настроены правила:
- `ClickHouse Failed Queries Rate` — `rate(ClickHouseProfileEvents_FailedQuery[5m]) > 0` в течение `2m`
- `ClickHouse Memory Resident High` — `MemoryResident / OSMemoryTotal * 100 > 85` в течение `5m`
- `ClickHouse Parts Active High` — `ClickHouseMetrics_PartsActive > 500` в течение `10m`

Проверка и reload без рестарта контейнера:

```bash
# Список правил unified alerting
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules

# Принудительно перечитать provisioning alerting
curl -s -X POST -u admin:admin http://localhost:3000/api/admin/provisioning/alerting/reload
```

### Troubleshooting мониторинга

- **"No data" в Grafana**: проверить, что Prometheus видит target (`Status -> Targets` в UI)
- **Метрики не обновляются**: ClickHouse экспортирует метрики на `0.0.0.0:9126` внутри сети Docker
- **Dashboard не загрузился**: проверить логи Grafana — provisioning работает при первом старте контейнера

## Troubleshooting

- `etl_pipeline` падает с ошибкой схемы: сначала запустить `ddl_init`.
- После `docker compose down -v` нужно повторно прогнать: `ddl_init` -> `kafka_load` -> `etl_pipeline`.
- Для демо по умолчанию использовать малый срез данных; полный прогон делать осознанно.
