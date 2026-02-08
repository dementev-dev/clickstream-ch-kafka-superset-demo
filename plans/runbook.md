# Runbook: запуск демо и загрузка данных

Этот документ фиксирует порядок действий и `make`‑таргеты. Он не описывает внутренности ClickHouse‑слоёв (это в `plans/clickhouse_ddl.md`).

## Предпосылки

- Docker + Docker Compose.
- Доступ к Docker daemon (если `docker compose ...` пишет `permission denied ... /var/run/docker.sock`, добавьте пользователя в группу `docker` или запускайте команды с правами, принятыми в вашей среде).

## Быстрый сценарий

1) Поднять инфраструктуру:

```bash
make up
```

2) Залить данные в Kafka (два варианта):

**Вариант А: Через Airflow DAG `kafka_load` (рекомендуется, фаза 2)**
```bash
# Через CLI — полная загрузка по умолчанию
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"reset_topics": true}'

# Ограниченная загрузка — первые 100 строк
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 100, "reset_topics": true}'

# Или через UI: Airflow → DAGs → kafka_load → Trigger DAG with config
```

Параметры `kafka_load`:
- `limit` — количество строк (default: 0 — все строки)
- `reset_topics` — пересоздать топики (default: true)
- `load_browser/load_location/load_device/load_geo` — выбор потоков (default: true)

**Вариант Б: Через shell-скрипт `make data` (устаревший)**
```bash
make data
```

`make data` не зависит от ClickHouse/DDL — достаточно, чтобы Kafka была поднята.

3) Применить DDL в ClickHouse:

```bash
make ddl
```

## Make таргеты

- `make up` — `docker compose up -d` (поднимает весь стек из `docker-compose.yml`).
- `make ddl` — применяет исполняемые SQL-файлы из `sql/ddl/*` в контейнер ClickHouse через `clickhouse-client`.
- `make data` — пересоздаёт топики (по умолчанию) и публикует события из `data/*.jsonl` в Kafka (1 строка = 1 Kafka message value).
- `make transform` — выполняет batch-процесс `STG -> ODS -> DDS -> DM` через `scripts/run_batch.sh`.

План реализации механики заливки (дизайн/решения): `plans/kafka_ingest_plan.md`.

План Airflow DAG'ов: `plans/airflow_dags_plan.md`.

## Загрузка данных в Kafka (`make data`)

### Топики

Скрипт использует фиксированный маппинг:

- `data/browser_events.jsonl` → `browser_events`
- `data/location_events.jsonl` → `location_events`
- `data/device_events.jsonl` → `device_events`
- `data/geo_events.jsonl` → `geo_events`

### Режимы загрузки

- По умолчанию — “debug срез”: первые 50 строк каждого файла.
- Полная загрузка — весь файл.

Параметры (env):

- `LIMIT` — сколько строк брать из каждого `.jsonl`. По умолчанию загружаются все записи (весь файл).
  Для ограничения используйте `LIMIT=50` или `LIMIT=100`.
- `RESET_TOPICS` — если `RESET_TOPICS=1` (по умолчанию), топики удаляются и создаются заново с теми же именами.
- `BOOTSTRAP_SERVER` — bootstrap для Kafka *изнутри kafka‑контейнера* (по умолчанию `kafka:29092`).

Примеры:

```bash
# Загрузить все данные (по умолчанию)
make data

# Быстрый тест — 50 строк на поток
LIMIT=50 make data

# Ограниченная загрузка — 100 строк на поток
LIMIT=100 make data

# Дозалить данные без пересоздания топиков
RESET_TOPICS=0 make data
```

## Применение DDL в ClickHouse (`make ddl`)

Скрипт исполняет SQL-файлы из `sql/ddl/*` по фиксированному порядку. Для `ENGINE = Kafka` важно, чтобы `kafka_broker_list` был доступен из контейнера ClickHouse.

В текущем compose:

- для соединений “контейнер → Kafka” используйте `kafka:29092`;
- `localhost:9092` подходит только для клиентов на хосте.
