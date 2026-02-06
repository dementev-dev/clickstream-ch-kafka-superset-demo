# ClickHouse Mini DWH для кликстрима

[![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20ClickHouse%20%7C%20Airflow%20%7C%20Superset-blue)](./docker-compose.yml)
[![Layers](https://img.shields.io/badge/layers-STG%20→%20ODS%20→%20DDS%20→%20DM-green)](./docs/ARCHITECTURE.md)
[![License](https://img.shields.io/badge/license-Educational-orange)]()

Многослойное хранилище данных (STG → ODS → DDS → DM) для анализа кликстрима e-commerce. 

Данные поступают из Kafka, проходят типизацию и обогащение, формируя витрины для BI-аналитики.

> **Соответствие заданию:** Реализован полный цикл Data Engineering: ingestion → хранилище со слоями → регулярный процесс трансформации → витрины для дашборда.

---

## 🚀 Быстрый старт

```bash
# 1. Поднять инфраструктуру (Kafka + ClickHouse + Superset)
make up

# 2. Создать структуру БД
make ddl

# 3. Загрузить данные (автоматически потекут STG → ODS)
make data           # первые 50 строк
# или: FULL=1 make data   # полный датасет (1000 строк)

# 4. Подождать 5-10 сек (данные проходят через Kafka)
sleep 10

# 5. Запустить batch-трансформацию (ODS → DDS → DM)
make transform
```

**Проверка:**
```bash
# Статистика по слоям
docker compose exec clickhouse clickhouse-client \
  --user=default --password=123456 --query="
    SELECT database, countDistinct(table) AS tables, sum(rows) AS rows
    FROM system.parts WHERE database IN ('stg','ods','dds','dm')
    GROUP BY database ORDER BY database
"

# Пример запроса к витрине
docker compose exec clickhouse clickhouse-client \
  --user=default --password=123456 --query="
    SELECT * FROM dm.v_utm_effectiveness ORDER BY clicks DESC LIMIT 5
"
```

---

## 📊 Доступные сервисы

| Сервис | URL | Назначение |
|--------|-----|------------|
| ClickHouse HTTP | http://localhost:9123/play | SQL-запросы |
| Kafka UI | http://localhost:8082 | Просмотр топиков |
| Airflow | http://localhost:8080 | Оркестрация ETL (admin/admin) |
| Superset | http://localhost:8088 | BI-дашборды |
| Prometheus | http://localhost:9090 | Метрики |
| Grafana | http://localhost:3000 | Визуализация метрик |

---

## 🏗️ Архитектура

```mermaid
flowchart TB
    subgraph Sources["📁 JSON файлы"]
        BE[browser_events.jsonl]
        LE[location_events.jsonl]
        DE[device_events.jsonl]
        GE[geo_events.jsonl]
    end

    subgraph Kafka["🚀 Kafka"]
        KT[Топики]
    end

    subgraph CH["🗄️ ClickHouse"]
        STG["STG — сырые JSON"]
        ODS["ODS — типизированные"]
        DDS["DDS — сущности"]
        DM["DM — витрины"]
    end

    subgraph Airflow["⚙️ Airflow"]
        DAG[ETL DAGs]
    end

    Sources -->|make data| Kafka -->|MV| STG -->|MV| ODS -->|Batch SQL| DDS -->|VIEW| DM
    DAG -.->|оркестрация| ODS & DDS & DM
```

**Поток данных:**
1. **STG** — сырые JSON из Kafka (MergeTree)
2. **ODS** — типизированные данные + DQ (ReplacingMergeTree)
3. **DDS** — собранные сущности event + click (Batch SQL)
4. **DM** — витрины для BI (VIEW)

[Подробное описание архитектуры →](./docs/ARCHITECTURE.md)

---

## 📁 Структура проекта

```
.
├── dags/             # Airflow DAGs для оркестрации
├── ddl/              # SQL для создания объектов (00_databases → 40_dm)
├── jobs/             # Batch-трансформации (ODS→DDS, DDS→DM)
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

## 🛠️ Команды Makefile

| Команда | Описание |
|---------|----------|
| `make up` | Поднять инфраструктуру |
| `make ddl` | Создать структуру БД |
| `make data` | Загрузить данные в Kafka (50 строк) |
| `FULL=1 make data` | Загрузить полный датасет |
| `make transform` | Запустить batch-процесс |

---

## 🔗 Ключи данных

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

## 📚 Документация

- [Архитектура и слои](./docs/ARCHITECTURE.md) — подробное описание STG/ODS/DDS/DM, ER-диаграммы, обоснование решений
- [DE-task.md](./data/DE-task.md) — исходное задание

---

## 🎯 Дашборд в Superset

1. Открыть http://localhost:8088
2. Database → Add:
   - **URI:** `clickhouse+connect://default:123456@clickhouse:8123/default`
3. Datasets → Add from `dm.v_*`
4. Charts & Dashboard

Основные витрины:
- `v_events_enriched` — полное обогащение
- `v_daily_traffic` — агрегация по дням
- `v_utm_effectiveness` — эффективность кампаний
- `v_top_pages_daily` — воронка страниц

---

## 🔮 Развитие проекта

### ✅ Реализовано
- [x] **Airflow** — оркестрация batch-процесса (инфраструктура готова, DAGs в разработке)

### 📋 В планах
- [ ] **Инкрементальный batch** — watermark-based загрузка
- [ ] **Материализация витрин** — для тяжёлых агрегаций
- [ ] **DQ мониторинг** — алерты на ошибки парсинга

---

## 📝 Лицензия

Проект создан для образовательных целей в рамках DE-тестового задания.
