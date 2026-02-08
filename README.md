# ClickHouse Mini DWH для кликстрима

[![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20ClickHouse%20%7C%20Airflow%20%7C%20Superset-blue)](./docker-compose.yml)
[![Layers](https://img.shields.io/badge/layers-STG%20→%20ODS%20→%20DDS%20→%20DM-green)](./docs/ARCHITECTURE.md)
[![License](https://img.shields.io/badge/license-Educational-orange)]()

Мини-демо для решения задания [DE-task.md](./data/DE-task.md): развернуть инфраструктуру на своей машине, прогнать кликстрим через Kafka в ClickHouse, сделать регулярный расчёт в Airflow и подготовить витрины под дашборд.

Фокус проекта: быстро показать работающий end-to-end сценарий и понятным языком объяснить, как устроены слои и почему пайплайн не падает на "грязных" данных.

Коротко про поток:
`data/*.jsonl` -> Kafka (1 строка = 1 сообщение) -> ClickHouse `stg` (сырые JSON) -> Airflow batch `stg -> ods -> dds -> dm` -> Superset.

---

## Быстрый старт (демо-сценарий)

```bash
# 1) Поднять инфраструктуру
make up

# Проверить статусы контейнеров
docker compose ps
```

Дальше основной путь идёт через Airflow (как в задании).

1. Открыть Airflow UI: `http://localhost:8080` (admin/admin)
2. Включить (unpause) и запустить `ddl_init` (создаёт базы/таблицы/VIEW в ClickHouse)

Опционально можно триггернуть DAG из CLI (удобно для CI/скрипта):
```bash
docker compose exec -T airflow-webserver airflow dags trigger ddl_init
```

Загрузка данных в Kafka (фаза 2 — через Airflow):
```bash
# Вариант 1: Через Airflow DAG (рекомендуется) — полная загрузка по умолчанию
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"reset_topics": true}'

# Ограниченная загрузка — первые 100 строк
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 100, "reset_topics": true}'

# Вариант 2: Через shell-скрипт (устаревший)
make data            # полная загрузка
```

Запуск batch-трансформации (STG -> ODS -> DDS -> DM) в Airflow (если DAG выключен, сначала unpause):
```bash
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline \
  --conf '{"full_refresh": true}'
```

Smoke-check результата в ClickHouse:
```bash
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query \
  "SELECT 'ods.browser_event' AS t, count() AS rows FROM ods.browser_event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query \
  "SELECT 'dds.click' AS t, count() AS rows FROM dds.click"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query \
  "SELECT 'dds.event' AS t, count() AS rows FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query \
  "SELECT 'dm.dq_summary' AS t, count() AS rows FROM dm.dq_summary"
```

---

## Доступные сервисы

| Сервис | URL | Назначение |
|--------|-----|------------|
| ClickHouse HTTP | http://localhost:9123/play | SQL-запросы |
| Kafka UI | http://localhost:8082 | Просмотр топиков |
| Airflow | http://localhost:8080 | Оркестрация ETL (admin/admin) |
| Superset | http://localhost:8088 | BI-дашборды |
| Prometheus | http://localhost:9090 | Метрики |
| Grafana | http://localhost:3000 | Визуализация метрик |

---

## Архитектура (в двух словах)

```mermaid
flowchart TB
    subgraph Sources["JSONL файлы"]
        BE[browser_events.jsonl]
        LE[location_events.jsonl]
        DE[device_events.jsonl]
        GE[geo_events.jsonl]
    end

    subgraph Kafka["Kafka"]
        KT[Топики]
    end

    subgraph CH["ClickHouse"]
        STG["stg: сырьё + Kafka MV"]
        ODS["ods: типизация + DQ"]
        DDS["dds: сущности"]
        DM["dm: витрины (VIEW)"]
    end

    subgraph Airflow["Airflow"]
            DAG[DAG: ddl_init / kafka_load / etl_pipeline]
        end

    Sources -->|make data| Kafka -->|MV| STG -->|Batch SQL| ODS -->|Batch SQL| DDS -->|VIEW| DM
    DAG -.->|оркестрация| STG & ODS & DDS & DM
```

Особенность задания про "грязные данные": парсинг не валит pipeline, ошибки фиксируются в `ods.*_errors` и в поле `parse_errors`.

[Подробное описание архитектуры →](./docs/ARCHITECTURE.md)

---

## Структура проекта

```
.
├── dags/             # Airflow DAGs для оркестрации
├── sql/
│   ├── ddl/          # DDL по слоям
│   │   ├── 00_databases.sql
│   │   ├── stg/10_stg.sql
│   │   ├── ods/20_ods.sql
│   │   ├── dds/30_dds.sql
│   │   └── dm/40_dm.sql
│   ├── ods/          # Batch SQL: STG -> ODS
│   ├── dds/          # Batch SQL: ODS -> DDS
│   └── dm/           # Batch SQL: DDS -> DM
├── scripts/          # Автоматизация (apply ddl, load data, run batch)
├── airflow/          # Конфигурация Airflow
│   └── requirements.txt
├── docs/             # Документация
│   └── ARCHITECTURE.md   # Подробное описание слоёв
├── data/             # Исходные JSONL файлы
├── docker-compose.yml
└── Makefile          # Команды: up, ddl, data, transform
```

---

## Команды Makefile

| Команда | Описание |
|---------|----------|
| `make up` | Поднять инфраструктуру |
| `make ddl` | Применить DDL в ClickHouse (вне Airflow) |
| `make data` | Загрузить данные в Kafka (50 строк) |
| `FULL=1 make data` | Загрузить полный датасет |
| `make transform` | Запустить batch-процесс `STG -> ODS -> DDS -> DM` (вне Airflow) |

Примечания про сохранность данных:
- Данные ClickHouse сохраняются в Docker volume `clickhouse-data`.
- Данные Kafka сохраняются в Docker volume `kafka-data`.
- `docker compose down` сохраняет named volumes, `docker compose down -v` удаляет их (и данные пропадут).

---

## Ключи данных (как джойним)

```mermaid
flowchart LR
    subgraph Sources["Источники"]
        BE["browser_events (event_id, click_id)"]
        LE["location_events (event_id)"]
        DE["device_events (click_id)"]
        GE["geo_events (click_id)"]
    end

    subgraph DDS["DDS"]
        EV["event (event_id PK)"]
        CL["click (click_id PK)"]
    end

    subgraph DM["DM"]
        V1[v_events_enriched]
        V2[v_daily_traffic]
        V3[v_utm_effectiveness]
    end

    BE -->|event_id| EV
    LE -->|event_id| EV
    BE -->|click_id| CL
    DE -->|click_id| CL
    GE -->|click_id| CL
    
    EV -->|LEFT JOIN click_id| V1
    CL --> V1
    EV --> V2 & V3
    CL --> V2 & V3
```

---

## Дашборд в Superset (опционально, но полезно)

1. Открыть `http://localhost:8088`
2. Database -> Add:
   - URI: `clickhouse+connect://default:123456@clickhouse:8123/default`
3. Создать datasets из `dm.v_*` (VIEW) и собрать несколько графиков

Идеи графиков под задание:
- Трафик по дням: `dm.v_daily_traffic` (events, uniq_users)
- Эффективность UTM: `dm.v_utm_effectiveness` (clicks, purchases)
- Популярные страницы: `dm.v_top_pages_daily` (pageviews)
- Качество данных: `dm.v_dq_errors_daily` (rows_cnt по error_code)

---

## Частые проблемы

- `etl_pipeline` падает с сообщением про схему: сначала запустите `ddl_init`.
- После `docker compose down -v` схема и данные исчезнут: нужно заново `ddl_init` и `make data`.
- Подключения используют разные протоколы:
  - Airflow (ClickHouseOperator) ходит в ClickHouse по native TCP (порт `9000` внутри сети Docker).
  - Superset (clickhouse-connect) ходит по HTTP (порт `8123` внутри сети Docker).

---

## Статус проекта

Реализовано (Этап 1):
- DAG `ddl_init`: последовательное применение DDL + проверка схемы.
- DAG `etl_pipeline`: precheck, ожидание данных в STG, batch-пересчёт ODS/DDS/DM, базовые проверки.
- Устойчивость к "грязным" данным: ошибки парсинга сохраняются в ODS, а не валят ingest.

В планах (не требуется для MVP задания):
- DAG `kafka_load` (чистый ingest из `.jsonl` в Kafka средствами Airflow).
- Инкрементальный batch (watermark вместо `full_refresh`).
- DQ мониторинг по расписанию.

---

## Документация

- [Архитектура и слои](./docs/ARCHITECTURE.md) — подробное описание STG/ODS/DDS/DM, ER-диаграммы, обоснование решений
- [DE-task.md](./data/DE-task.md) — исходное задание
