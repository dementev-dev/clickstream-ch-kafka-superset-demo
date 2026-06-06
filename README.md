# ClickHouse Mini DWH для кликстрима

[![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20ClickHouse%20%7C%20Airflow%20%7C%20Superset%20%7C%20Prometheus%2FGrafana-blue)](./docker-compose.yml)
[![Layers](https://img.shields.io/badge/layers-STG%20→%20ODS%20→%20DDS%20→%20DM-green)](./docs/ARCHITECTURE.md)
[![License](https://img.shields.io/badge/license-Educational-orange)]()

Мини-демо для решения задания [DE-task.md](./docs/DE-task.md): развернуть инфраструктуру на своей машине, прогнать кликстрим через Kafka в ClickHouse, сделать регулярный расчёт в Airflow и подготовить витрины под дашборд.

Фокус проекта: быстро показать работающий end-to-end сценарий и понятным языком объяснить, как устроены слои и почему пайплайн не падает на "грязных" данных.

Коротко про поток:
`data/*.jsonl` -> Airflow DAG `kafka_load` -> Kafka (1 строка = 1 сообщение) -> ClickHouse `stg` (сырые JSON) -> Airflow DAG `etl_pipeline` (`stg -> ods -> dds -> dm`) -> Superset.

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

Загрузка данных в Kafka через Airflow DAG:
```bash
# Полная загрузка (по умолчанию limit=0)
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"reset_topics": true}'

# Ограниченная загрузка — первые 100 строк
docker compose exec -T airflow-webserver airflow dags trigger kafka_load \
  --conf '{"limit": 100, "reset_topics": true}'
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

| Сервис | URL | Назначение | Логин/Пароль |
|--------|-----|------------|--------------|
| ClickHouse HTTP | http://localhost:9123/play | SQL-запросы | default/123456 |
| Kafka UI | http://localhost:8082 | Просмотр топиков | — |
| Airflow | http://localhost:8080 | Оркестрация ETL | admin/admin |
| Superset | http://localhost:8088 | BI-дашборды | admin/admin |
| Prometheus | http://localhost:9090 | Метрики | — |
| Grafana | http://localhost:3000 | Визуализация метрик | admin/admin |

Superset: UI доступен после `make up` по адресу http://localhost:8088, дашборд — http://localhost:8088/superset/dashboard/1/

---

## Подключение DBeaver (кратко)

После прогона `ddl_init -> kafka_load -> etl_pipeline` можно быстро проверить витрины в DBeaver (удобно для демо бизнесу).

1. `Database -> New Database Connection -> ClickHouse`
2. Параметры:
   - `Host`: `localhost`
   - `Port`: `9123` (HTTP)
   - `Database`: `default`
   - `Username`: `default`
   - `Password`: `123456`
3. Нажать `Test Connection` -> `Finish`

Если ваш драйвер просит native-протокол, используйте порт `8002`.

Полезные быстрые запросы для первичного анализа:
```sql
SELECT count() AS rows FROM dm.v_events_enriched;
SELECT * FROM dm.v_daily_traffic ORDER BY event_date DESC LIMIT 20;
SELECT * FROM dm.v_utm_effectiveness ORDER BY clicks DESC LIMIT 20;
```

---

## Архитектура (в двух словах)

```mermaid
flowchart LR
    subgraph AF["Airflow"]
        D1["ddl_init"]
        D2["kafka_load"]
        D3["etl_pipeline"]
    end

    subgraph Kafka["Kafka"]
        Topics[4 топика]
    end

    subgraph CH["ClickHouse"]
        STG["STG: сырые данные"]
        ODS["ODS: типизация + DQ"]
        DDS["DDS: сущности"]
        DM["DM: витрины VIEW"]
    end

    D2 -->|загрузка JSONL| Kafka -->|Kafka MV| STG
    STG -->|batch| ODS -->|batch| DDS -->|VIEW| DM

    D1 -.->|DDL| CH
    D3 -.->|batch| ODS & DDS
```

Особенность задания про "грязные данные": парсинг не валит pipeline, ошибки фиксируются в `ods.*_errors` и в поле `parse_errors`.

[Подробное описание архитектуры →](./docs/ARCHITECTURE.md)

---

## Структура проекта

```
.
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
├── configs/          # Конфигурации сервисов
│   ├── superset_config.py      # Конфиг Superset (PostgreSQL metadata)
│   ├── prometheus/             # Prometheus конфигурация
│   └── grafana/                # Grafana dashboards & datasources
├── scripts/          # Служебные shell-скрипты (legacy fallback, не основной путь)
├── airflow/          # Конфигурация Airflow
│   ├── dags/         # Airflow DAGs для оркестрации
│   └── requirements.txt
├── superset/         # Скрипты инициализации Superset
│   ├── init_superset.py        # Подключение к ClickHouse + датасеты
│   └── create_dashboard.py     # Создание дашборда с чартами
├── docs/             # Документация
│   └── ARCHITECTURE.md   # Подробное описание слоёв
├── data/             # Исходные JSONL файлы
├── docker-compose.yml
└── Makefile          # Команды: up, ddl, transform, superset-*
```

---

## Команды Makefile

| Команда | Описание |
|---------|----------|
| `make up` | Поднять инфраструктуру |
| `make ddl` | Применить DDL в ClickHouse (вне Airflow) |
| `make transform` | Запустить batch-процесс `STG -> ODS -> DDS -> DM` (вне Airflow) |
| `make superset-init` | Подключение к ClickHouse + импорт датасетов |
| `make superset-dashboard` | Создание дашборда с чартами, обновление layout/metadata |
| `make superset-ui` | Показать URL Superset |
| `make superset-restart` | Перезапуск Superset |

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

Superset развёрнут с автоматической инициализацией: подключение к ClickHouse, датасеты и дашборд создаются автоматически при первом запуске.

### Быстрый доступ

| URL | Назначение | Логин/Пароль |
|-----|------------|--------------|
| http://localhost:8088 | Superset UI | admin/admin |
| http://localhost:8088/superset/dashboard/1/ | Готовый дашборд | — |

### Автоматическая инициализация (рекомендуется)

```bash
# При первом запуске инфраструктуры
make up

# Дашборд создаётся автоматически через 30-60 секунд
# Проверить готовность:
curl http://localhost:8088/health  # должно вернуть 200
```

Что создаётся автоматически:
- **Подключение к ClickHouse**: `clickhouse_dwh` (URI: `clickhousedb://default:123456@clickhouse:8123/default`)
- **Датасеты** (6 шт.): `v_events_enriched`, `v_daily_traffic`, `v_utm_effectiveness`, `v_top_pages_daily`, `v_session_overview`, `dq_summary`
- **Чарты** (10 шт.): KPI метрики, графики трафика, география, UTM-эффективность, прохождение строк по слоям
- **Дашборд**: "🛒 E-commerce Analytics Dashboard"

### Ручная инициализация (если автоматика не сработала)

```bash
# Подключение к ClickHouse + датасеты
make superset-init

# Создание дашборда с чартами
make superset-dashboard
```

### Структура дашборда

Дашборд "E-commerce Analytics Dashboard" включает:

| Блок | Чарты | Датасет |
|------|-------|---------|
| **KPI** | Total Events, Unique Users, Avg Events/Visit, Conversion to /confirmation | `v_events_enriched` |
| **Динамика** | Events over Time (timeline), Traffic by Device (pie) | `v_events_enriched` |
| **География** | Geography Map по странам | `v_events_enriched` |
| **Маркетинг** | UTM Effectiveness Table, Page Funnel | `v_utm_effectiveness`, `v_top_pages_daily` |
| **Слои** | Rows by Layer (event) — прохождение строк STG→ODS→DDS→DM | `dq_summary` |

Фильтр `Date Range` по умолчанию открыт как `No filter`, потому что демо-данные лежат в
историческом диапазоне (`2022-11-28`).

### Архитектура Superset

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Superset UI   │────▶│   PostgreSQL     │───▶│   ClickHouse    │
│   (localhost)   │     │   (metadata)     │     │   (данные)      │
│     :8088       │     │  dashboards,     │     │  dm.v_* VIEW    │
└─────────────────┘     │  datasets, charts│     └─────────────────┘
                        └──────────────────┘
```

Особенности конфигурации:
- **Metadata**: PostgreSQL (shared с Airflow) — данные сохраняются при перезапуске
- **Data**: ClickHouse через `clickhouse-connect` (HTTP порт 8123)
- **Config**: `configs/superset_config.py` (PostgreSQL URI, секретный ключ)

---

## Мониторинг инфраструктуры (Grafana + Prometheus)

Для оценки состояния ClickHouse доступен дашборд мониторинга:

1. Открыть Grafana: `http://localhost:3000` (admin/admin)
2. Дашборд "ClickHouse Overview" загружается автоматически
3. Проверить метрики Prometheus: `http://localhost:9090` → Status → Targets

Что отслеживается:
- System Health: CPU, Memory (Resident/Code)
- Query Performance: queries/sec, active queries, failed queries
- MergeTree Storage: parts count, merge rate

Что алертится:
- Failed queries rate (`rate(ClickHouseProfileEvents_FailedQuery[5m]) > 0`)
- Memory Resident > 85% от `OSMemoryTotal`
- Active parts > 500

Конфигурация provisioning находится в [`configs/grafana/provisioning/`](configs/grafana/provisioning/). Подробнее в [`docs/OPERATIONS.md`](docs/OPERATIONS.md#мониторинг).

---

## Частые проблемы

- `etl_pipeline` падает с сообщением про схему: сначала запустите `ddl_init`.
- После `docker compose down -v` схема и данные исчезнут: нужно заново запустить `ddl_init`, затем `kafka_load`, затем `etl_pipeline`.
- Подключения используют разные протоколы:
  - Airflow (ClickHouseOperator) ходит в ClickHouse по native TCP (порт `9000` внутри сети Docker).
  - Superset (clickhouse-connect) ходит по HTTP (порт `8123` внутри сети Docker).
- **Superset**: дашборд не появился сразу — подождите 30-60 секунд после `make up`, затем проверьте `curl http://localhost:8088/health`.
- **Superset**: после `make clean` витрины `dm.*` ещё не созданы, поэтому чарты могут быть пустыми до запуска `ddl_init -> kafka_load -> etl_pipeline`; после этого выполните `make superset-init`.
- **Superset**: при полном сбросе (`docker compose down -v`) метаданные Superset пропадут т.к. используется общая PostgreSQL. Для чистого перезапуска Superset удалите только БД `superset` в PostgreSQL и перезапустите контейнеры.

---

## Статус проекта

Реализовано:
- **Инфраструктура**: Kafka + ClickHouse + Airflow + Superset + Prometheus/Grafana
- **Ingest**: DAG `ddl_init` (DDL + проверка схемы), DAG `kafka_load` (параметры `limit`, `reset_topics`)
- **Трансформации**: DAG `etl_pipeline` (pre-check, batch STG→ODS→DDS→DM, валидация)
- **Витрины**: VIEW в DM для бизнес-дашбордов (трафик, UTM, качество данных)
- **Superset**: Автоматическая инициализация (подключение ClickHouse, 6 датасетов, 10 чартов, дашборд), метаданные в PostgreSQL
- **Надёжность**: ошибки парсинга сохраняются в ODS, пайплайн не падает на "грязных" данных
- **Мониторинг**: Prometheus скрейпит ClickHouse метрики, Grafana дашборд и alert rules

В планах (не требуется для MVP задания):
- Инкрементальный batch (watermark вместо `full_refresh`)
- DQ мониторинг по расписанию

---

## Документация

- [Архитектура и слои](./docs/ARCHITECTURE.md) — подробное описание STG/ODS/DDS/DM, ER-диаграммы, обоснование решений
- [DE-task.md](./docs/DE-task.md) — исходное задание
