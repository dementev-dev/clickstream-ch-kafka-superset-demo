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
    subgraph AF["Airflow"]
        DAG1["ddl_init"]
        DAG2["kafka_load (bootstrap)"]
        DAG3["etl_pipeline"]
    end

    subgraph GEN["Generator"]
        G["generator-service (steady-stream)"]
    end

    subgraph Kafka["Kafka"]
        K[4 топика]
    end

    subgraph STG["STG"]
        S[*_raw таблицы]
    end

    subgraph ODS["ODS"]
        O[*_event таблицы]
        OE[error_tables]
    end

    subgraph DDS["DDS"]
        D1[event]
        D2[click]
    end

    subgraph DM["DM"]
        V[витрины VIEW]
    end

    DAG2 -->|bootstrap JSONL| K
    G -->|stream events| K
    K -->|MV| S
    S -->|batch| O
    O -->|argMax + JOIN| D1 & D2
    O -.->|ошибки| OE
    D1 & D2 -->|VIEW| V

    DAG1 -.->|DDL| STG & ODS & DDS & DM
    DAG3 -.->|batch| ODS & DDS
```

В учебном стенде предусмотрены два пути ingest:

- `bootstrap`: DAG `kafka_load` для разового/контрольного прогона из `data/*.jsonl`;
- `steady-stream`: автономный генератор, публикующий события в Kafka непрерывно.

### Слои и их назначение

```mermaid
flowchart LR
    subgraph AF["Airflow"]
        DAG1["ddl_init"]
        DAG2["kafka_load (bootstrap)"]
        DAG3["etl_pipeline"]
    end

    subgraph GEN["Generator"]
        G["generator-service"]
    end

    subgraph L1["STG"]
        KAFKA["Kafka Engine"]
        STG_T["*_raw таблицы"]
    end

    subgraph L2["ODS"]
        ODS_T["Типизированные таблицы"]
        DQ["parse_errors"]
    end

    subgraph L3["DDS"]
        DDS_T["event + click"]
    end

    subgraph L4["DM"]
        DM_T["VIEW"]
    end

    DAG2 -->|bootstrap JSONL| KAFKA
    G -->|steady-stream| KAFKA
    KAFKA -->|MV| STG_T
    STG_T -->|batch| ODS_T
    ODS_T -->|argMax + JOIN| DDS_T -->|VIEW| DM_T
    ODS_T -.->|ошибки| DQ

    DAG1 -.->|DDL| L1 & L2 & L3 & L4
    DAG3 -.->|batch| ODS_T & DDS_T
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
| `*_errors` | — | MergeTree | Копия строк с любой ошибкой разбора: Kafka-метаданные + `raw` + `error_reason` (не типизированная копия события) |

**Batch шаг наполнения ODS:**

| Шаг | Назначение |
|-----|-----------|
| `load_ods` (`sql/ods/20_stg_to_ods.sql`) | Полная пересборка ODS из STG в рамках DAG `etl_pipeline` |
| `TRUNCATE ods.*` | Очистка перед пересборкой для детерминированного результата |
| `INSERT ... SELECT` | Типизация валидных строк в основные ODS таблицы |
| `INSERT ... SELECT` в `*_errors` | Сохранение копии строк с любой ошибкой разбора (Kafka-метаданные + `raw` + `error_reason`) |

**Логика разделения (DQ-split):**
- **Основная таблица** `ods.*`: строки с валидным бизнес-ключом (`WHERE key IS NOT NULL`). Ошибка по *неключевому* полю не выкидывает строку — она остаётся, но помечается в массиве `parse_errors`.
- **Таблица ошибок** `ods.*_errors`: **копия** строк, где при разборе случилась *любая* ошибка (отдельный `INSERT ... SELECT`). У неё своя схема — не типизированное событие, а сырьё для разбора.
- Условия пересекаются нарочно: строка с валидным ключом, но битым неключевым полем попадает **и в основную таблицу, и в `*_errors`**. Подробный разбор — в разделе [«Обработка ошибок в ODS»](#обработка-ошибок-в-ods-dq-split) и в уроке 2 курса.

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
- **Управляемость**: шаг `load_ods` виден в Airflow, есть task-level мониторинг и ретраи

---

### DDS (Detailed Data Store)

**Назначение:** Собранные сущности для аналитики.

| Таблица | PK | Источники | JOIN-ключ |
|---------|-----|-----------|-----------|
| `event` | event_id | browser_event (ведущая) + location_event (LEFT) | click_id → click |
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
| `v_session_overview` | Сессионная аналитика (только идентифицированные пользователи, `user_domain_id IS NOT NULL`) | День × пользователь × сессия |
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
- Хранит статистику по всем слоям (`stg`/`ods`/`dds`/`dm`): `total_rows` по таблицам, `rows_with_errors` в ODS, `orphan_events` в DDS (события без своего клика) и `total_rows` финальной витрины `dm.v_events_enriched` — чтобы lineage замыкался на одном (event) зерне вплоть до DM

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
    participant Compose as Docker Compose
    participant Airflow as Airflow
    participant Gen as generator-service
    participant K as Kafka
    participant CH as ClickHouse
    participant STG as stg.*_raw
    participant ODS as ods.*
    participant DDS as dds.*
    participant DM as dm.*

    User->>Compose: make up
    Compose->>K: docker compose up -d kafka
    Compose->>CH: docker compose up -d clickhouse
    Compose->>Airflow: docker compose up -d airflow-*
    Compose->>Gen: docker compose up -d generator
    Compose-->>User: ✅ Инфраструктура готова

    User->>Airflow: Trigger ddl_init
    Airflow->>CH: sql/ddl/00_databases.sql
    Airflow->>CH: sql/ddl/stg/10_stg.sql (Kafka Engine + MV)
    Airflow->>CH: sql/ddl/ods/20_ods.sql
    Airflow->>CH: sql/ddl/dds/30_dds.sql
    Airflow->>CH: sql/ddl/dm/40_dm.sql
    CH-->>User: ✅ Структура БД создана

    alt Bootstrap режим
        User->>Airflow: Trigger kafka_load
        Airflow->>K: precheck + prepare_topics
        loop 4 файла
            Airflow->>K: KafkaProducer.send(topic, json_line)
        end
        K-->>User: ✅ Данные в Kafka
    else Streaming режим
        loop каждые 1-10 секунд
            Gen->>K: send N_t (Poisson) в 4 топика
        end
        K-->>User: ✅ Непрерывный поток в Kafka
    end

    K->>CH: Потребление сообщений
    CH->>STG: INSERT через MV
    CH-->>User: ✅ Данные в STG

    User->>Airflow: Trigger etl_pipeline
    Airflow->>CH: sql/ods/20_stg_to_ods.sql
    Airflow->>CH: sql/dds/30_ods_to_dds.sql
    CH->>ODS: argMax() — снапшот
    CH->>DDS: JOIN + INSERT
    Airflow->>CH: sql/dm/40_dds_to_dm.sql
    CH->>DM: DQ summary
    CH-->>User: ✅ ODS/DDS/DM обновлены
```

---

## Связи ключей

### ER-диаграмма

```mermaid
erDiagram
    BROWSER_EVENT ||--o| LOCATION_EVENT : "event_id (LEFT)"
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
        String os_timezone
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

**event** (browser + location), `browser_event` — ведущая таблица:
```mermaid
flowchart LR
    subgraph ODS["ODS"]
        B["browser_event<br/>(ведущая)"]
        L["location_event"]
    end

    subgraph DDS["DDS"]
        EV["event"]
    end

    B -->|все event_id| EV
    L -.->|LEFT JOIN event_id| EV
```

**Важно:** сборка идёт **от browser** через `LEFT JOIN location`. `event_id`, которые есть только в `location_event` (без browser), в `dds.event` **не попадают**. Если у события нет своей location-строки — событие остаётся, поля страницы/UTM пустые (`NULL`), и в `ods_parse_errors` ставится маркер `location_not_found`. Тот же принцип, что и в `dds.click`: не теряем, а оставляем видимый след. Подробнее — урок 3 курса (`docs/course/lessons/03_ods_to_dds.md`).

**click** (device + geo) с поддержкой partial data:
```mermaid
flowchart LR
    subgraph ODS["ODS"]
        D["device_by_click"]
        G["geo_by_click"]
    end

    subgraph BUILD["Batch SQL"]
        U["UNION DISTINCT click_id"]
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
2. Фильтруем NULL в batch SQL (`WHERE key IS NOT NULL` → основная таблица)
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

### Обработка ошибок в ODS (DQ-split)

**Проблема:** Грязные данные могут содержать не только невалидные ключи, но и невалидные timestamp/координаты/ID.

**Решение — DQ-split:**
1. **Основная таблица** `ods.*`: строки с валидным бизнес-ключом (`WHERE key IS NOT NULL`). Ошибки по *неключевым* полям не выкидывают строку — она остаётся, но помечается массивом `parse_errors`.
2. **Таблица ошибок** `ods.*_errors`: **копия** строк, где при разборе случилась *любая* ошибка. Это не типизированная копия события, а сырьё для разбора — метаданные доставки из Kafka, исходный JSON и причина ошибки:
   - `ingest_ts`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_ts` — координаты сообщения в Kafka;
   - `raw` — исходный JSON «как пришёл»;
   - `error_reason` — список несработавших полей одной строкой (`arrayStringConcat(parse_errors, ',')`).
3. **DQ-метрики**: массив `parse_errors` в основной таблице для аудита.

**Одна строка может попасть в оба места — это не баг, а замысел.** Строка с валидным ключом, но битым неключевым полем (например, валидный `event_id`, но `event_ts IS NULL`) и **остаётся** в основной таблице (с меткой в `parse_errors`), и **копируется** в `*_errors`. Основная таблица отвечает на вопрос «что есть для работы», таблица ошибок — «что пришло битым и требует разбора». Подробный разбор — в уроке 2 курса (`docs/course/lessons/02_stg_to_ods.md`).

```sql
-- event_id, event_ts, click_id, parse_errors — это не колонки stg.browser_raw,
-- а алиасы из блока WITH, где raw (сырой JSON) разбирается через JSONExtract*/*OrNull.
-- Здесь WITH опущен для краткости; полная версия — в sql/ods/20_stg_to_ods.sql.

-- Основная таблица: валидный ключ; parse_errors помечает битые неключевые поля
INSERT INTO ods.browser_event
SELECT ..., parse_errors FROM stg.browser_raw WHERE event_id IS NOT NULL;

-- Таблица ошибок: другая схема (Kafka-метаданные + raw + error_reason);
-- сюда едет копия любой строки с хотя бы одной ошибкой разбора
INSERT INTO ods.browser_event_errors
SELECT ingest_ts, kafka_topic, kafka_partition, kafka_offset, kafka_ts,
       raw, arrayStringConcat(parse_errors, ',') AS error_reason
FROM stg.browser_raw
WHERE length(parse_errors) > 0
  AND (event_id IS NULL OR event_ts IS NULL OR click_id IS NULL);
```

### Partial data в DDS

**Проблема:** Device и geo события приходят независимо (не все click_id есть в обоих источниках).

**Решение:**
1. **UNION DISTINCT** всех click_id из обоих источников
2. **LEFT JOIN** для получения данных (обрабатываем device-only и geo-only)
3. **DQ-маркеры** в `ods_parse_errors`: `device_not_found`, `geo_not_found` (нет соответствующего источника по `click_id`), `geo_country_missing` (гео есть, но страна не определена)

### Перечень DQ-маркеров

Маркеры качества копятся в массивах `parse_errors` (ODS) и `ods_parse_errors` (DDS). Полный список:

| Слой / таблица | Маркеры | Когда ставится |
|----------------|---------|----------------|
| ODS `browser_event` | `bad_event_id`, `bad_event_timestamp`, `bad_click_id` | поле не разобралось в нужный тип |
| ODS `location_event` | `bad_event_id` | не разобрался `event_id` |
| ODS `device_by_click` | `bad_click_id`, `bad_user_domain_id` | не разобрались `click_id` / `user_domain_id` |
| ODS `geo_by_click` | `bad_click_id`, `bad_geo_latitude`, `bad_geo_longitude` | не разобрались ключ или координаты |
| DDS `click` | `device_not_found`, `geo_not_found`, `geo_country_missing` | нет источника по `click_id` либо страна не определена |
| DDS `event` | `location_not_found` | у события нет своей location-строки |

В DDS наследуются `parse_errors` только **ведущего** источника (device → `dds.click`, browser → `dds.event`); к ним добавляются маркеры стыковки (`*_not_found`, `*_missing`). `parse_errors` из geo/location в DDS не переносятся.

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

Инфраструктура Airflow развёрнута и отвечает за DDL/ETL.  
Генератор работает отдельно и не управляется через Airflow DAG-и.

```python
# airflow/dags/ddl_init_dag.py — создание баз/таблиц (ручной запуск при bootstrap)
# airflow/dags/kafka_load_dag.py — bootstrap-загрузка JSONL в Kafka (через kafka-python)
# airflow/dags/etl_pipeline_dag.py — основной ETL (STG→ODS→DDS→DM)

# Учебный формат:
# - DDL и трансформации выполняются явными SQL-task через ClickHouseOperator;
# - SQL-файлы вызываются по фиксированным путям;
# - ingest может идти двумя путями:
#   1) bootstrap через DAG `kafka_load`;
#   2) непрерывный поток через автономный `generator-service`.
#
# Базовый demo-сценарий:
# ddl_init -> kafka_load -> etl_pipeline
# Расширенный учебный сценарий:
# generator-service (continuous) + периодический etl_pipeline
```

**DAG `kafka_load`**:
- Загрузка данных из `data/*.jsonl` в Kafka через `kafka-python`
- TaskGroup `precheck`: проверка Kafka, файлов, параметров
- TaskGroup `ingest`: создание топиков → параллельная загрузка 4 потоков → проверка
- Параметры: `limit` (0 = все), `reset_topics`

**Подключение к ClickHouse:**
- Connection: `clickhouse_default`
- URL: `clickhouse://default:123456@clickhouse:9000/default` (native TCP для Airflow plugin)
- Provider/интеграция: `airflow-clickhouse-plugin` (в `airflow/requirements.txt`), задачи выполняются через `ClickHouseOperator`.
- Дополнительно: `kafka-python==2.0.6` для работы с Kafka из DAG.
- Примечание: Superset подключается к ClickHouse по HTTP (обычно `clickhousedb://...:8123/...`).

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
