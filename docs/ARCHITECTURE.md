# Архитектура ClickHouse Mini DWH

Подробное описание слоёв хранилища, потоков данных и принятых решений.

---

## Содержание

1. [Обзор архитектуры](#обзор-архитектуры)
2. [Слои хранилища](#слои-хранилища)
   - [STG (Staging)](#stg-staging)
   - [ODS (Operational Data Store)](#ods-operational-data-store)
   - [DDS (Detailed Data Store)](#dds-detailed-data-store)
   - [DM (Data Marts)](#dm-data-marts)
3. [Поток данных](#поток-данных)
4. [Связи ключей](#связи-ключей)
5. [Принятые решения](#принятые-решения)
6. [Масштабирование](#масштабирование)

---

## Обзор архитектуры

### Общая схема потока данных

```mermaid
flowchart LR
    subgraph Sources["📁 Источники (JSONL)"]
        BE[browser_events]
        LE[location_events]
        DE[device_events]
        GE[geo_events]
    end

    subgraph Kafka["🚀 Kafka"]
        K1[browser_events]
        K2[location_events]
        K3[device_events]
        K4[geo_events]
    end

    subgraph STG["📦 STG"]
        S1[browser_raw]
        S2[location_raw]
        S3[device_raw]
        S4[geo_raw]
        MV1[mv_*_to_ods]
        MV2[mv_*_to_errors]
    end

    subgraph ODS["🔧 ODS"]
        O1[browser_event]
        O2[location_event]
        O3[device_by_click]
        O4[geo_by_click]
        OE[error_tables]
    end

    subgraph DDS["🎯 DDS"]
        DE1[event]
        DC1[click]
    end

    subgraph DM["📊 DM"]
        DM1[v_events_enriched]
        DM2[v_daily_traffic]
        DM3[v_utm_effectiveness]
        DM4[v_top_pages]
    end

    BE --> K1 --> S1 --> MV1 --> O1 --> DE1 --> DM1
    LE --> K2 --> S2 --> MV1 --> O2 --> DE1
    DE --> K3 --> S3 --> MV1 --> O3 --> DC1 --> DM1
    GE --> K4 --> S4 --> MV1 --> O4 --> DC1
    
    S1 & S2 & S3 & S4 --> MV2 -.-> OE
    DE1 --> DM2 & DM3 & DM4
    DC1 --> DM2 & DM3 & DM4
```

### Слои и их назначение

```mermaid
flowchart TB
    subgraph L0["📝 Источники"]
        RAW["JSON файлы (1000 строк)"]
    end

    subgraph L1["📦 STG - Staging"]
        direction LR
        KAFKA["Kafka Engine"]
        STG_T["*_raw таблицы<br/>(MergeTree)"]
    end

    subgraph L2["🔧 ODS - Операционный слой"]
        direction LR
        ODS_T["Типизированные таблицы<br/>(ReplacingMergeTree)"]
        DQ["parse_errors<br/>DQ-метрики"]
    end

    subgraph L3["🎯 DDS - Детальный слой"]
        DDS_T["event + click<br/>(Batch SQL)"]
    end

    subgraph L4["📊 DM - Витрины"]
        DM_T["VIEW для BI<br/>(Superset/Grafana)"]
    end

    RAW -->|kafka-console-producer| KAFKA -->|MV| STG_T -->|MV| ODS_T
    ODS_T -->|argMax + JOIN| DDS_T -->|VIEW| DM_T
    ODS_T -.->|ошибки| DQ
```

---

## Слои хранилища

### STG (Staging)

**Назначение:** Сохранение сырых данных "как есть" для воспроизводимости и отладки.

| Таблица | Движок | Описание |
|---------|--------|----------|
| `browser_raw` | MergeTree | Сырые события браузера |
| `location_raw` | MergeTree | Сырые данные страниц/UTM |
| `device_raw` | MergeTree | Сырые данные устройств |
| `geo_raw` | MergeTree | Сырые гео-данные |
| `kafka_*_raw` | Kafka | Таблицы-источники Kafka |
| `mv_kafka_*_to_stg` | MV | Поток из Kafka в STG |

**Структура таблицы:**
```sql
CREATE TABLE stg.browser_raw (
    ingest_ts DateTime64(3),
    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    kafka_ts DateTime64(3),
    raw String  -- ← JSON как есть
)
```

**Почему так:**
- **Повторяемость**: если в ODS ошибка — можно перестроить без перезагрузки из Kafka
- **Отладка**: видеть "что реально пришло" vs "что распарсилось"
- **DQ**: невалидные JSON не ломают pipeline

---

### ODS (Operational Data Store)

**Назначение:** Типизированные данные с дедупликацией и DQ-метриками.

| Таблица | Ключ | Движок | Описание |
|---------|------|--------|----------|
| `browser_event` | event_id | ReplacingMergeTree(src_ingest_ts) | События браузера |
| `location_event` | event_id | ReplacingMergeTree(src_ingest_ts) | Данные страниц |
| `device_by_click` | click_id | ReplacingMergeTree(src_ingest_ts) | Устройства |
| `geo_by_click` | click_id | ReplacingMergeTree(src_ingest_ts) | Гео-данные |
| `*_errors` | — | MergeTree | Строки с битыми ключами |

**Materialized Views для обработки ошибок:**

| MV | Назначение |
|----|-----------|
| `mv_browser_raw_to_ods_errors` | Переносит строки с ошибками в `browser_event_errors` |
| `mv_location_raw_to_ods_errors` | Переносит строки с ошибками в `location_event_errors` |
| `mv_device_raw_to_ods_errors` | Переносит строки с ошибками в `device_by_click_errors` |
| `mv_geo_raw_to_ods_errors` | Переносит строки с ошибками в `geo_by_click_errors` |

**Логика разделения:**
- **Основная таблица**: строки с валидными ключами (`WHERE key IS NOT NULL`)
- **Таблица ошибок**: строки с невалидными ключами (`WHERE key IS NULL`)

**Пример структуры:**
```sql
CREATE TABLE ods.browser_event (
    event_id Nullable(UUID),
    event_ts Nullable(DateTime64(6)),
    event_date Date MATERIALIZED ifNull(toDate(event_ts), toDate(src_ingest_ts)),
    event_type LowCardinality(Nullable(String)),
    click_id Nullable(UUID),
    browser_name LowCardinality(Nullable(String)),
    src_ingest_ts DateTime64(3),
    src_raw String,
    parse_errors Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
ORDER BY (event_id)
SETTINGS allow_nullable_key = 1;
```

**DQ-контроль:**
```sql
-- Проверка ошибок парсинга
SELECT 
    arrayJoin(parse_errors) AS error,
    count() AS cnt
FROM ods.browser_event
GROUP BY error;
```

**Почему так:**
- **Изоляция источников**: изменения в одном не ломают другие
- **Версионирование**: `ReplacingMergeTree` хранит последнюю версию по `src_ingest_ts`
- **Nullable ключи**: `allow_nullable_key = 1` позволяет хранить "битые" строки

---

### DDS (Detailed Data Store)

**Назначение:** Собранные сущности для аналитики.

| Таблица | PK | Источники | JOIN-ключ |
|---------|-----|-----------|-----------|
| `event` | event_id | browser_event + location_event | click_id → click |
| `click` | click_id | device_by_click + geo_by_click | — |

**Структура:**
```sql
CREATE TABLE dds.event (
    event_id UUID,
    event_ts Nullable(DateTime64(6)),
    event_type LowCardinality(Nullable(String)),
    click_id Nullable(UUID),
    page_url Nullable(String),
    page_url_path LowCardinality(Nullable(String)),
    utm_source LowCardinality(Nullable(String)),
    browser_name LowCardinality(Nullable(String)),
    -- ... все поля из browser + location
    dds_update_ts DateTime64(3),
    ods_parse_errors Array(LowCardinality(String))
);

CREATE TABLE dds.click (
    click_id UUID,
    user_domain_id Nullable(UUID),
    device_type LowCardinality(Nullable(String)),
    geo_country LowCardinality(Nullable(String)),
    -- ... все поля из device + geo
    dds_update_ts DateTime64(3),
    ods_parse_errors Array(LowCardinality(String))
);
```

**Загрузка (Batch SQL):**

Загрузка `dds.click` с поддержкой partial data (когда device и geo приходят независимо):

```sql
-- UNION всех click_id из device и geo
INSERT INTO dds.click
SELECT 
    c.click_id,
    d.user_domain_id,
    d.device_type,
    g.geo_country,
    g.geo_latitude,
    -- ... остальные поля
    now64(3) AS dds_update_ts,
    arrayFilter(x -> x != '', arrayConcat(
        ifNull(d.parse_errors, []),
        if(d.click_id IS NULL, ['device_not_found'], []),
        if(g.click_id IS NULL, ['geo_not_found'], [])
    )) AS ods_parse_errors
FROM (
    -- Union всех click_id для обработки geo-only и device-only
    SELECT click_id FROM (
        SELECT assumeNotNull(click_id) AS click_id
        FROM ods.device_by_click WHERE click_id IS NOT NULL
        GROUP BY click_id
    )
    UNION DISTINCT
    SELECT click_id FROM (
        SELECT assumeNotNull(click_id) AS click_id
        FROM ods.geo_by_click WHERE click_id IS NOT NULL
        GROUP BY click_id
    )
) AS c
LEFT JOIN (
    -- Снапшот device
    SELECT assumeNotNull(click_id) AS click_id, ...
    FROM ods.device_by_click GROUP BY click_id
) AS d ON d.click_id = c.click_id
LEFT JOIN (
    -- Снапшот geo
    SELECT assumeNotNull(click_id) AS click_id, ...
    FROM ods.geo_by_click GROUP BY click_id
) AS g ON g.click_id = c.click_id;
```

**Ключевые особенности:**
- **UNION click_id**: собираем все уникальные click_id из обоих источников
- **LEFT JOIN**: обрабатываем случаи когда есть только device или только geo
- **`assumeNotNull`**: типобезопасное преобразование после фильтрации NULL
- **DQ-метрики**: маркируем отсутствующие данные (`device_not_found`, `geo_not_found`)

**Почему batch, а не MV:**
- **Согласованность**: MV с JOIN даёт eventual consistency (данные приходят в разное время)
- **Контроль**: Batch SQL можно проверить, откатить, перезапустить
- **Масштабируемость**: легко сделать инкрементальный batch

---

### DM (Data Marts)

**Назначение:** Витрины для BI-инструментов.

| Витрина | Назначение | Гранулярность |
|---------|-----------|---------------|
| `v_events_enriched` | Полное обогащение | 1 строка = 1 событие |
| `v_daily_traffic` | Агрегация трафика | День × страна × устройство × браузер × UTM |
| `v_top_pages_daily` | Популярность страниц | День × URL path |
| `v_utm_effectiveness` | Маркетинговая аналитика | День × UTM source/medium/campaign |
| `v_session_overview` | Сессионная аналитика | День × пользователь × сессия |
| `v_dq_errors_daily` | Мониторинг качества | День × тип ошибки |

**Пример:**
```sql
CREATE VIEW dm.v_events_enriched AS
SELECT
    e.*,
    c.user_domain_id,
    c.device_type,
    c.geo_country,
    arrayConcat(e.ods_parse_errors, c.ods_parse_errors) AS parse_errors
FROM dds.event AS e
LEFT JOIN dds.click AS c ON c.click_id = e.click_id;
```

**Материализованная таблица DQ:**

```sql
-- Таблица для мониторинга качества (пересоздаётся при каждом batch)
TRUNCATE TABLE dm.dq_summary;
INSERT INTO dm.dq_summary
SELECT today() AS check_date, 'stg' AS layer, ...
FROM ...
```

- `TRUNCATE` предотвращает накопление дубликатов при повторных запусках
- Хранит статистику по всем слоям (stg/ods/dds) для быстрой проверки

**Почему VIEW:**
- Для демо: достаточно производительности
- Гибкость: изменения логики не требуют пересоздания таблиц
- Для продакшена: можно материализовать тяжёлые агрегации

---

## Поток данных

### Sequence диаграмма процесса

```mermaid
sequenceDiagram
    participant User as Пользователь
    participant Make as Makefile
    participant K as Kafka
    participant CH as ClickHouse
    participant STG as stg.*_raw
    participant ODS as ods.*
    participant DDS as dds.*
    participant DM as dm.*

    User->>Make: make up
    Make->>K: docker compose up kafka
    Make->>CH: docker compose up clickhouse
    K-->>User: ✅ Инфраструктура готова

    User->>Make: make ddl
    Make->>CH: sql/ddl/00_databases.sql
    Make->>CH: sql/ddl/stg/10_stg.sql (Kafka Engine)
    Make->>CH: sql/ddl/ods/20_ods.sql (MV)
    Make->>CH: sql/ddl/dds/30_dds.sql
    Make->>CH: sql/ddl/dm/40_dm.sql
    CH-->>User: ✅ Структура БД создана

    User->>Make: make data
    Make->>K: load_kafka_data.sh
    K->>K: Создание топиков
    loop 4 файла
        Make->>K: kafka-console-producer
    end
    K->>CH: Потребление сообщений
    CH->>STG: INSERT через MV
    STG->>ODS: INSERT через MV (типизация)
    K-->>User: ✅ Данные в Kafka
    CH-->>User: ✅ Данные в STG/ODS

    User->>Make: make transform
    Make->>CH: sql/dds/30_ods_to_dds.sql
    CH->>ODS: argMax() — снапшот
    CH->>DDS: JOIN + INSERT
    Make->>CH: sql/dm/40_dds_to_dm.sql
    CH->>DM: DQ summary
    CH-->>User: ✅ DDS/DM обновлены
```

---

## Связи ключей

### ER-диаграмма

```mermaid
erDiagram
    BROWSER_EVENT ||--|| LOCATION_EVENT : "event_id"
    BROWSER_EVENT ||--o| DEVICE_BY_CLICK : "click_id"
    BROWSER_EVENT ||--o| GEO_BY_CLICK : "click_id"
    
    BROWSER_EVENT {
        UUID event_id PK
        DateTime event_ts
        String event_type
        UUID click_id FK
        String browser_name
        String browser_user_agent
        String browser_language
    }
    
    LOCATION_EVENT {
        UUID event_id PK
        String page_url
        String page_url_path
        String referer_url
        String referer_medium
        String utm_source
        String utm_medium
        String utm_campaign
    }
    
    DEVICE_BY_CLICK {
        UUID click_id PK
        String os
        String os_name
        String device_type
        UInt8 device_is_mobile
        String user_custom_id
        UUID user_domain_id
    }
    
    GEO_BY_CLICK {
        UUID click_id PK
        Float64 geo_latitude
        Float64 geo_longitude
        String geo_country
        String geo_timezone
        String geo_region_name
        String ip_address
    }
```

### Сборка DDS-сущностей

**event** (browser + location):
```mermaid
flowchart LR
    subgraph ODS["ODS"]
        B["browser_event"]
        L["location_event"]
    end

    subgraph DDS["DDS"]
        EV["event"]
    end

    B -->|JOIN по event_id| EV
    L -->|JOIN по event_id| EV
```

**click** (device + geo) с поддержкой partial data:
```mermaid
flowchart LR
    subgraph ODS["ODS"]
        D["device_by_click"]
        G["geo_by_click"]
    end

    subgraph BUILD["Batch SQL"]
        U["UNION DISTINCT<br/>click_id"]
        J["LEFT JOIN"]
    end

    subgraph DDS["DDS"]
        CL["click"]
    end

    D -->|все click_id| U
    G -->|все click_id| U
    U --> J
    D -->|данные| J
    G -->|данные| J
    J --> CL
```

**Важно:** Не все `click_id` из events есть в device/geo. Используем `LEFT JOIN`.

---

## Принятые решения

### Почему `allow_nullable_key = 1`?

В ClickHouse ключ сортировки не может быть NULL по умолчанию. Но в "грязных" данных ключи могут отсутствовать.

**Решение:**
1. Включаем `allow_nullable_key = 1` в `ReplacingMergeTree`
2. Фильтруем NULL в MV (`WHERE key IS NOT NULL` → основная таблица)
3. Отдельные `*_errors` таблицы для NULL-ключей

### Почему `ReplacingMergeTree`?

- Дедупликация по бизнес-ключу
- Версионирование по timestamp (последняя версия wins)
- Фоновый merge не блокирует чтение

### Почему batch ODS→DDS?

| Подход | Плюсы | Минусы |
|--------|-------|--------|
| **MV + JOIN** | Реалтайм | Eventual consistency, дубли при late arrival |
| **Batch (выбрано)** | Согласованность, контроль | Задержка до следующего запуска |

### Обработка ошибок в ODS

**Проблема:** Грязные данные с невалидными ключами (NULL event_id/click_id).

**Решение:**
1. **Основная таблица**: только валидные строки (`WHERE key IS NOT NULL`)
2. **Таблица ошибок**: строки с невалидными ключами через отдельные MV
3. **DQ-метрики**: массив `parse_errors` для аудита

```sql
-- Основная таблица
CREATE MV mv_browser_raw_to_ods_browser_event
TO ods.browser_event
SELECT ... FROM stg.browser_raw WHERE event_id IS NOT NULL;

-- Таблица ошибок
CREATE MV mv_browser_raw_to_ods_errors
TO ods.browser_event_errors
SELECT ... FROM stg.browser_raw WHERE event_id IS NULL;
```

### Partial data в DDS

**Проблема:** Device и geo события приходят независимо (не все click_id есть в обоих источниках).

**Решение:**
1. **UNION DISTINCT** всех click_id из обоих источников
2. **LEFT JOIN** для получения данных (обрабатываем device-only и geo-only)
3. **DQ-маркеры**: `device_not_found`, `geo_not_found` в `parse_errors`

---

## Масштабирование

### Инкрементальный batch

Вместо полного `TRUNCATE + INSERT`:

```sql
-- Добавить watermark
INSERT INTO dds.click
SELECT ...
FROM ods.device_by_click
WHERE src_ingest_ts > (
    SELECT max(dds_update_ts) FROM dds.click
);
```

### Материализация витрин

Для тяжёлых агрегаций:

```sql
-- Создать таблицу вместо VIEW
CREATE TABLE dm.daily_traffic AS
SELECT * FROM dm.v_daily_traffic;

-- Пересчёт по расписанию
TRUNCATE TABLE dm.daily_traffic;
INSERT INTO dm.daily_traffic SELECT * FROM dm.v_daily_traffic;
```

### Airflow-оркестрация

Инфраструктура Airflow развёрнута и готова к использованию:

```python
# dags/ddl_init_dag.py и dags/etl_pipeline_dag.py
#
# Учебный формат:
# - DDL и трансформации выполняются явными SQL-task через ClickHouseOperator;
# - SQL-файлы вызываются по фиксированным путям;
# - загрузка данных в Kafka (Этап 1) выполняется через `make data`.
#
# Основной demo-сценарий:
# ddl_init -> make data -> etl_pipeline
```

**Подключение к ClickHouse:**
- Connection: `clickhouse_default`
- URL: `clickhouse://default:123456@clickhouse:9000/default` (native TCP для Airflow plugin)
- Provider/интеграция: `airflow-clickhouse-plugin` (в `airflow/requirements.txt`), задачи выполняются через `ClickHouseOperator`.
- Примечание: Superset подключается к ClickHouse по HTTP (обычно `clickhouse+connect://...:8123/...`).

---

## Полезные запросы

### Проверка слоёв

```sql
-- Статистика по слоям
SELECT 
    database,
    countDistinct(table) AS tables,
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(bytes)) AS size
FROM system.parts
WHERE database IN ('stg', 'ods', 'dds', 'dm')
GROUP BY database
ORDER BY database;
```

### DQ-анализ

```sql
-- Ошибки парсинга по слоям
SELECT 
    'ods.browser_event' AS table,
    countIf(length(parse_errors) > 0) AS errors,
    count() AS total
FROM ods.browser_event
UNION ALL
SELECT 
    'dds.event',
    countIf(length(ods_parse_errors) > 0),
    count()
FROM dds.event;
```

### Воронка конверсии

```sql
SELECT 
    page_url_path,
    pageviews,
    uniq_clicks,
    round(uniq_clicks * 100.0 / lag(uniq_clicks) OVER (ORDER BY pageviews DESC), 2) AS conversion_pct
FROM dm.v_top_pages_daily
ORDER BY pageviews DESC;
```
