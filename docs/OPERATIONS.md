# Operations Runbook

Операционный runbook для локального запуска и проверки пайплайна.

## Локальный запуск

Базовые команды:

- `make up` (или `docker compose up -d`)
- `make down` (остановить и удалить контейнеры/сети проекта)
- `make clean` (полная очистка: `down -v --remove-orphans`)
- `make ddl` (применяет SQL из `sql/ddl/00_databases.sql` и `sql/ddl/*/*.sql` в ClickHouse)
- `make data` (пересоздаёт топики и заливает данные в Kafka; по умолчанию полный объём, срез — `LIMIT=50 make data`)
- `make transform` (запускает batch-процесс ODS -> DDS -> DM)
- `make superset-init` (повторная инициализация Superset: подключение к ClickHouse, датасеты, дашборд)
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
- Гейт целостности DDS: `check_dds_integrity` считает события без клика, а
  `assert_dds_integrity` роняет DAG при `orphan_events > 0`. Проверка идёт после
  `load_dds` и до `load_dm_summary`, чтобы DM не собирался поверх нарушенной связи
  `dds.event -> dds.click`. Для `assert_dds_integrity` задано `retries=0`: повтор не
  чинит уже собранную сироту и только задерживает явный failed-статус.

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

### TL;DR после `git pull`

```bash
# Быстрый вариант (make)
make reload-monitoring
# Если мониторинг "залип" (No data/out of bounds) — жесткое восстановление:
make recover-monitoring

# Или вручную:
docker compose up -d prometheus grafana kafka-exporter statsd-exporter
docker compose restart prometheus statsd-exporter
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload
```

Если менялся `configs/prometheus_ch.xml`: `docker compose restart clickhouse`.

### Prometheus + Grafana для ClickHouse, Kafka и Airflow

Стек мониторинга поднимается вместе с остальной инфраструктурой:

```bash
# Проверить статус сервисов мониторинга
docker compose ps prometheus grafana

# Проверить скрейп ClickHouse в Prometheus
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

### Конфигурация

- **ClickHouse**: встроенный Prometheus endpoint (`/metrics` на порту `9126`)
- **Kafka**: через `kafka-exporter` (порт `9308`)
- **Airflow**: через `statsd-exporter` (StatsD → Prometheus, порт `9102`)
  - Airflow отправляет метрики в StatsD-формате на `statsd-exporter:8125`
  - Mapping конфигурация: `configs/statsd_mapping.yml`
- **Grafana provisioning** (`configs/grafana/provisioning/`):
  - Дашборды: ClickHouse Overview, Kafka Overview, Airflow Overview
  - Алерты: ClickHouse, Kafka, Airflow

### После `git pull`: быстрый апдейт мониторинга

Если прилетели изменения в `configs/grafana/provisioning/*` или `configs/prometheus.yml`, примените их так:

```bash
# Рекомендуемый способ (через make)
make reload-monitoring
# Если метрики пропали/залипли:
make recover-monitoring

# Или вручную:
docker compose rm -sf prometheus statsd-exporter
docker compose up -d prometheus grafana kafka-exporter statsd-exporter
docker compose restart airflow-scheduler airflow-webserver
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload
```

Проверка результата:

```bash
# Дашборды и алерты
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | grep -o '"title":"[^"]*"'

# Kafka метрики
curl -s http://localhost:9090/api/v1/targets | grep kafka
curl -s http://localhost:9308/metrics | grep "^kafka_brokers"
```

Если в пулле изменился `configs/prometheus_ch.xml`, дополнительно перезапустите ClickHouse:

```bash
docker compose restart clickhouse
```

Если дашборд Kafka не загрузился (ошибка "Dashboard title cannot be empty" в логах), пересоздайте контейнер Grafana:

```bash
docker compose stop grafana && docker compose rm -f grafana && docker compose up -d grafana
```

### Дашборд ClickHouse Overview

URL: `http://localhost:3000/d/clickhouse-overview/clickhouse-overview`

| Раздел | Метрики |
|--------|---------|
| System Health | CPU Usage, Memory Resident, Memory Code |
| Query Performance | Queries per Second, Active Queries, Failed Queries (total), Total Queries, Inserted Rows/sec |
| MergeTree Storage | Total Parts, Parts by State, Total Merges, Merges per Second |

Принятое решение по метрикам: сверили naming через Context7 (`/clickhouse/clickhouse-docs`, раздел Prometheus interface) и заменили недоступные в `25.1` серии на фактически экспортируемые (`ClickHouseProfileEvents_InsertedRows`, `ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables`, `ClickHouseMetrics_Parts*`).

### Дашборд Kafka Overview

URL: `http://localhost:3000/d/kafka-overview/kafka-overview`

| Раздел | Метрики |
|--------|---------|
| Cluster Health | Brokers Up, Topics, Total Partitions, Consumer Groups |
| Throughput | Messages In / sec by Topic |
| Consumers | Consumer Lag by Group |
| Partitions | Partition Offsets (Current) |

**Источник метрик:** `kafka-exporter` (danielqsj/kafka-exporter), формат конфигурации подтверждён через Context7 (`/danielqsj/kafka_exporter`, `/prometheus/docs`).

### Проверка метрик

```bash
# Prometheus собирает метрики ClickHouse
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseAsyncMetrics_MemoryResident"
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseProfileEvents_Query"

# Prometheus собирает метрики Kafka
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers"
curl -s "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag"

# Прямая проверка kafka-exporter
curl -s http://localhost:9308/metrics | grep "^kafka_"
```

```bash
# Проверить, что Prometheus собирает метрики
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseAsyncMetrics_MemoryResident"

# Проверить счётчик запросов
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseProfileEvents_Query"
```

### Алерты Grafana

**ClickHouse Alerts** — provisioning-файл: `configs/grafana/provisioning/alerting/clickhouse-alert-rules.yml`

Настроены правила:
- `ClickHouse Failed Queries Rate` — `rate(ClickHouseProfileEvents_FailedQuery[5m]) > 0` в течение `2m`
- `ClickHouse Memory Resident High` — `MemoryResident / OSMemoryTotal * 100 > 85` в течение `5m`
- `ClickHouse Parts Active High` — `ClickHouseMetrics_PartsActive > 500` в течение `10m`

**Kafka Alerts** — provisioning-файл: `configs/grafana/provisioning/alerting/kafka-alert-rules.yml`

Настроены правила:
- `Kafka Broker Down` — `kafka_brokers < 1` в течение `1m`
- `Kafka Consumer Lag High` — `kafka_consumergroup_lag > 10000` в течение `5m`
- `Kafka No Messages Produced` — `rate(kafka_topic_partition_current_offset[5m]) < 0.1` в течение `10m`
- `Kafka Consumer Group Missing` не включён: для демо-стенда даёт шум на стартовых прогонах и не повышает диагностику по сравнению с lag/throughput.

Проверка и reload без рестарта контейнера:

```bash
# Список правил unified alerting
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules

# Принудительно перечитать provisioning alerting
curl -s -X POST -u admin:admin http://localhost:3000/api/admin/provisioning/alerting/reload
```

### Troubleshooting мониторинга

**Общие проблемы:**
- **"No data" в Grafana**: проверить, что Prometheus видит target (`Status -> Targets` в UI)
- **Dashboard не загрузился**: проверить логи Grafana — provisioning работает при первом старте контейнера
- **Prometheus spam `out of bounds` и дашборды пустые**: выполнить `make recover-monitoring`

**ClickHouse:**
- **Метрики не обновляются**: ClickHouse экспортирует метрики на `0.0.0.0:9126` внутри сети Docker

**Kafka:**
- **`connection refused` к Kafka**: проверить, что kafka-exporter использует `kafka:29092` (внутренняя сеть), не `localhost:9092`
- **Метрики Kafka не появляются**: проверить, что kafka-exporter подключился к Kafka — `docker compose logs kafka-exporter`
- **Нет консьюмер-групп**: kafka-exporter показывает lag только при наличии активных консьюмеров с закоммиченными offset

## Troubleshooting

- `etl_pipeline` падает с ошибкой схемы: сначала запустить `ddl_init`.
- `git pull` падает с `Permission denied` на `data/*` или `configs/grafana/provisioning/*`:
  - Причина: локально есть файлы/каталоги не вашего пользователя (часто после запуска контейнеров с root-пользователем).
  - Диагностика:
    ```bash
    ls -ld data configs/grafana/provisioning
    ls -l data | head -n 20
    ```
  - Быстрое восстановление:
    ```bash
    # Владелец и права для рабочей копии репозитория
    sudo chown -R "$USER:$USER" .
    find . -type d -exec chmod u+rwx {} \;
    find . -type f -exec chmod u+rw {} \;
    ```
  - После восстановления повторить `git pull --ff-only`.
  - Не запускать `git` через `sudo`.
- `grafana` перезапускается с ошибкой `attempt to write a readonly database`:
  - Причина: старый `grafana_lib` содержит `grafana.db`, созданный root-пользователем.
  - Простой recovery (сбросить только volume Grafana):
    ```bash
    docker compose stop grafana
    docker volume ls | grep grafana_lib
    docker volume rm <project>_grafana_lib
    docker compose up -d grafana
    ```
- В Superset ошибки `DB engine Error` и `Cannot load filter`, а в логах есть `Can't load plugin: sqlalchemy.dialects:clickhouse.connect`:
  - Причина: некорректный URI диалекта ClickHouse (`clickhouse+connect://...`).
  - Используйте URI `clickhousedb://...` и пересоберите сервисы Superset:
    ```bash
    docker compose build superset superset-init
    docker compose up -d clickhouse
    docker compose up -d --force-recreate superset-init superset
    ```
- После `docker compose down -v` нужно повторно прогнать: `ddl_init` -> `kafka_load` -> `etl_pipeline`.
- После `make clean`/`down -v` Superset стартует, но витрины `dm.*` ещё пустые или отсутствуют до прогона ETL; после `ddl_init` -> `kafka_load` -> `etl_pipeline` выполнить `make superset-init`.
- Для демо по умолчанию использовать малый срез данных; полный прогон делать осознанно.
