-- ============================================================================
-- Слой ODS (Operational Data Store) — типизированные данные + дедупликация + DQ
-- ============================================================================
-- Назначение:
--   - Типизация данных из STG (String → UUID, DateTime, etc.)
--   - Дедупликация через ReplacingMergeTree (последняя версия по src_ingest_ts)
--   - Контроль качества: массив parse_errors для "грязных" данных
--   - Разделение: валидные строки → основная таблица, ошибки → *_errors
--
-- Поток данных:
--   STG (*_raw) → MV → ODS (основная таблица + error_tables)
-- ============================================================================

-- ============================================================================
-- BROWSER EVENTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные строки (event_id IS NOT NULL)
-- ----------------------------------------------------------------------------
-- ReplacingMergeTree: при мердже оставляет строку с максимальным src_ingest_ts
-- allow_nullable_key = 1: разрешаем NULL в ключе (ClickHouse по умолчанию запрещает)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods.browser_event
(
    event_id           Nullable(UUID),                     -- UUID события (ключ)
    event_ts           Nullable(DateTime64(6)),            -- Время события из JSON
    event_date         Date MATERIALIZED ifNull(toDate(event_ts), toDate(src_ingest_ts)),  -- Партиция
    event_type         LowCardinality(Nullable(String)),   -- Тип события (pageview, click и т.д.)
    click_id           Nullable(UUID),                     -- Связь с click-контекстом
    browser_name       LowCardinality(Nullable(String)),   -- Chrome, Firefox и т.д.
    browser_user_agent Nullable(String),                   -- User-Agent строка
    browser_language   LowCardinality(Nullable(String)),   -- Язык браузера
    src_ingest_ts      DateTime64(3),                      -- Время загрузки в ODS (версия для ReplacingMergeTree)
    src_raw            String,                             -- Исходный JSON для аудита
    parse_errors       Array(LowCardinality(String))       -- Ошибки парсинга (если есть)
)
ENGINE = ReplacingMergeTree(src_ingest_ts)  -- Движок дедупликации по версии
PARTITION BY toYYYYMM(event_date)           -- Партиции по месяцу для быстрой очистки
ORDER BY (event_id)                         -- Ключ сортировки (и дедупликации)
SETTINGS allow_nullable_key = 1;            -- Разрешаем NULL в ключе (для "битых" данных)

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с невалидными ключами (event_id IS NULL)
-- ----------------------------------------------------------------------------
-- Сохраняем полную информацию для анализа проблем с данными
-- MergeTree без Replacing: сохраняем все ошибки (не дедуплицируем)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods.browser_event_errors
(
    ingest_ts       DateTime64(3),            -- Время вставки в ClickHouse
    kafka_topic     LowCardinality(String),   -- Топик Kafka (для отслеживания источника)
    kafka_partition Int32,                    -- Партиция Kafka
    kafka_offset    Int64,                    -- Смещение Kafka (уникальный идентификатор сообщения)
    kafka_ts        DateTime64(3),            -- Время из Kafka
    raw             String,                   -- Исходный JSON
    error_reason    LowCardinality(String)    -- Описание ошибки (что именно не распарсилось)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, kafka_topic, kafka_partition, kafka_offset);

-- ----------------------------------------------------------------------------
-- MV: STG → ODS (основная таблица)
-- ----------------------------------------------------------------------------
-- Фильтруем только валидные строки: WHERE event_id IS NOT NULL
-- Парсим JSON, типизируем поля, собираем ошибки в массив parse_errors
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_browser_raw_to_ods
TO ods.browser_event
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'event_id')) AS event_id,
    parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'event_timestamp'), 6) AS event_ts,
    JSONExtractString(raw, 'event_type') AS event_type,
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    JSONExtractString(raw, 'browser_name') AS browser_name,
    JSONExtractString(raw, 'browser_user_agent') AS browser_user_agent,
    JSONExtractString(raw, 'browser_language') AS browser_language
SELECT
    event_id,
    event_ts,
    event_type,
    click_id,
    browser_name,
    browser_user_agent,
    browser_language,
    ingest_ts AS src_ingest_ts,
    raw AS src_raw,
    -- Собираем ошибки парсинга в массив (пустые строки фильтруем)
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', ''),
        if(event_ts IS NULL, 'bad_event_timestamp', ''),
        if(click_id IS NULL, 'bad_click_id', '')
    ]) AS parse_errors
FROM stg.browser_raw
WHERE event_id IS NOT NULL;  -- Только валидные строки (NULL → в error_tables)

-- ----------------------------------------------------------------------------
-- MV: STG → ODS (таблица ошибок)
-- ----------------------------------------------------------------------------
-- Перенаправляем строки с ошибками парсинга в отдельную таблицу
-- Это позволяет не терять данные и анализировать проблемы
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_browser_raw_to_ods_errors
TO ods.browser_event_errors
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'event_id')) AS event_id,
    parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'event_timestamp'), 6) AS event_ts,
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', ''),
        if(event_ts IS NULL, 'bad_event_timestamp', ''),
        if(click_id IS NULL, 'bad_click_id', '')
    ]) AS parse_errors
SELECT
    ingest_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    raw,
    arrayStringConcat(parse_errors, ',') AS error_reason
FROM stg.browser_raw
WHERE length(parse_errors) > 0           -- Есть хотя бы одна ошибка
    AND (
        event_id IS NULL
        OR event_ts IS NULL
        OR click_id IS NULL
    );

-- ============================================================================
-- LOCATION EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.location_event
(
    event_id       Nullable(UUID),
    page_url       Nullable(String),              -- Полный URL страницы
    page_url_path  LowCardinality(Nullable(String)),  -- Путь (/home, /product и т.д.)
    referer_url    Nullable(String),              -- Откуда пришёл пользователь
    referer_medium LowCardinality(Nullable(String)),  -- Тип referer (internal, search и т.д.)
    utm_medium     LowCardinality(Nullable(String)),  -- UTM medium (cpc, organic и т.д.)
    utm_source     LowCardinality(Nullable(String)),  -- UTM source (google, mailchimp и т.д.)
    utm_content    LowCardinality(Nullable(String)),  -- UTM content (ad_1, ad_2 и т.д.)
    utm_campaign   LowCardinality(Nullable(String)),  -- UTM campaign (campaign_1 и т.д.)
    src_ingest_ts  DateTime64(3),
    src_raw        String,
    parse_errors   Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
PARTITION BY toYYYYMM(toDate(src_ingest_ts))  -- Партиция по времени загрузки (нет event_date)
ORDER BY (event_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS ods.location_event_errors
(
    ingest_ts       DateTime64(3),
    kafka_topic     LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    kafka_ts        DateTime64(3),
    raw             String,
    error_reason    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, kafka_topic, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_location_raw_to_ods
TO ods.location_event
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'event_id')) AS event_id
SELECT
    event_id,
    JSONExtractString(raw, 'page_url') AS page_url,
    JSONExtractString(raw, 'page_url_path') AS page_url_path,
    JSONExtractString(raw, 'referer_url') AS referer_url,
    JSONExtractString(raw, 'referer_medium') AS referer_medium,
    JSONExtractString(raw, 'utm_medium') AS utm_medium,
    JSONExtractString(raw, 'utm_source') AS utm_source,
    JSONExtractString(raw, 'utm_content') AS utm_content,
    JSONExtractString(raw, 'utm_campaign') AS utm_campaign,
    ingest_ts AS src_ingest_ts,
    raw AS src_raw,
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', '')
    ]) AS parse_errors
FROM stg.location_raw
WHERE event_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_location_raw_to_ods_errors
TO ods.location_event_errors
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'event_id')) AS event_id,
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', '')
    ]) AS parse_errors
SELECT
    ingest_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    raw,
    arrayStringConcat(parse_errors, ',') AS error_reason
FROM stg.location_raw
WHERE length(parse_errors) > 0
    AND event_id IS NULL;

-- ============================================================================
-- DEVICE EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.device_by_click
(
    click_id         Nullable(UUID),
    os               Nullable(String),              -- Полное название ОС
    os_name          LowCardinality(Nullable(String)),  -- Короткое название (Windows, iOS и т.д.)
    os_timezone      LowCardinality(Nullable(String)),  -- Таймзона пользователя
    device_type      LowCardinality(Nullable(String)),  -- Mobile, Computer, Tablet
    device_is_mobile Nullable(UInt8),                 -- 1 = мобильное, 0 = десктоп
    user_custom_id   Nullable(String),               -- Email или username
    user_domain_id   Nullable(UUID),                 -- UUID пользователя в системе
    src_ingest_ts    DateTime64(3),
    src_raw          String,
    parse_errors     Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
PARTITION BY toYYYYMM(toDate(src_ingest_ts))
ORDER BY (click_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS ods.device_by_click_errors
(
    ingest_ts       DateTime64(3),
    kafka_topic     LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    kafka_ts        DateTime64(3),
    raw             String,
    error_reason    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, kafka_topic, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_device_raw_to_ods
TO ods.device_by_click
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    JSONExtract(raw, 'device_is_mobile', 'Nullable(UInt8)') AS device_is_mobile,
    toUUIDOrNull(JSONExtractString(raw, 'user_domain_id')) AS user_domain_id
SELECT
    click_id,
    JSONExtractString(raw, 'os') AS os,
    JSONExtractString(raw, 'os_name') AS os_name,
    JSONExtractString(raw, 'os_timezone') AS os_timezone,
    JSONExtractString(raw, 'device_type') AS device_type,
    device_is_mobile,
    JSONExtractString(raw, 'user_custom_id') AS user_custom_id,
    user_domain_id,
    ingest_ts AS src_ingest_ts,
    raw AS src_raw,
    arrayFilter(x -> x != '', [
        if(click_id IS NULL, 'bad_click_id', ''),
        if(user_domain_id IS NULL, 'bad_user_domain_id', '')
    ]) AS parse_errors
FROM stg.device_raw
WHERE click_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_device_raw_to_ods_errors
TO ods.device_by_click_errors
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    toUUIDOrNull(JSONExtractString(raw, 'user_domain_id')) AS user_domain_id,
    arrayFilter(x -> x != '', [
        if(click_id IS NULL, 'bad_click_id', ''),
        if(user_domain_id IS NULL, 'bad_user_domain_id', '')
    ]) AS parse_errors
SELECT
    ingest_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    raw,
    arrayStringConcat(parse_errors, ',') AS error_reason
FROM stg.device_raw
WHERE length(parse_errors) > 0
    AND (
        click_id IS NULL
        OR user_domain_id IS NULL
    );

-- ============================================================================
-- GEO EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.geo_by_click
(
    click_id        Nullable(UUID),
    geo_latitude    Nullable(Float64),              -- Широта
    geo_longitude   Nullable(Float64),              -- Долгота
    geo_country     LowCardinality(Nullable(String)),  -- Код страны (RU, US и т.д.)
    geo_timezone    LowCardinality(Nullable(String)),  -- Таймзона
    geo_region_name Nullable(String),               -- Название региона/города
    ip_address      Nullable(String),               -- IP адрес
    src_ingest_ts   DateTime64(3),
    src_raw         String,
    parse_errors    Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
PARTITION BY toYYYYMM(toDate(src_ingest_ts))
ORDER BY (click_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS ods.geo_by_click_errors
(
    ingest_ts       DateTime64(3),
    kafka_topic     LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    kafka_ts        DateTime64(3),
    raw             String,
    error_reason    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, kafka_topic, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_geo_raw_to_ods
TO ods.geo_by_click
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    toFloat64OrNull(JSONExtractString(raw, 'geo_latitude')) AS geo_latitude,
    toFloat64OrNull(JSONExtractString(raw, 'geo_longitude')) AS geo_longitude
SELECT
    click_id,
    geo_latitude,
    geo_longitude,
    JSONExtractString(raw, 'geo_country') AS geo_country,
    JSONExtractString(raw, 'geo_timezone') AS geo_timezone,
    JSONExtractString(raw, 'geo_region_name') AS geo_region_name,
    JSONExtractString(raw, 'ip_address') AS ip_address,
    ingest_ts AS src_ingest_ts,
    raw AS src_raw,
    arrayFilter(x -> x != '', [
        if(click_id IS NULL, 'bad_click_id', ''),
        if(geo_latitude IS NULL, 'bad_geo_latitude', ''),
        if(geo_longitude IS NULL, 'bad_geo_longitude', '')
    ]) AS parse_errors
FROM stg.geo_raw
WHERE click_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_geo_raw_to_ods_errors
TO ods.geo_by_click_errors
AS
WITH
    toUUIDOrNull(JSONExtractString(raw, 'click_id')) AS click_id,
    toFloat64OrNull(JSONExtractString(raw, 'geo_latitude')) AS geo_latitude,
    toFloat64OrNull(JSONExtractString(raw, 'geo_longitude')) AS geo_longitude,
    arrayFilter(x -> x != '', [
        if(click_id IS NULL, 'bad_click_id', ''),
        if(geo_latitude IS NULL, 'bad_geo_latitude', ''),
        if(geo_longitude IS NULL, 'bad_geo_longitude', '')
    ]) AS parse_errors
SELECT
    ingest_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    raw,
    arrayStringConcat(parse_errors, ',') AS error_reason
FROM stg.geo_raw
WHERE length(parse_errors) > 0
    AND (
        click_id IS NULL
        OR geo_latitude IS NULL
        OR geo_longitude IS NULL
    );
