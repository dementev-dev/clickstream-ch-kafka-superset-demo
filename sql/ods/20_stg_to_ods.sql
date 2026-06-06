-- ============================================================================
-- Batch-трансформация: STG → ODS
-- ============================================================================
-- Поток данных:
--   stg.*_raw  →  ods.*  (валидный ключ)  +  ods.*_errors  (любая ошибка)
--
-- Назначение:
--   - Перенос типизации STG → ODS из Materialized View в управляемый batch
--   - Полная пересборка ODS для прозрачного мониторинга в Airflow
--   - Сохранение DQ-логики: parse_errors + отдельные *_errors таблицы
--
-- DQ-split (почему одна строка может попасть в оба места):
--   - Основная таблица ods.*  — строки с валидным КЛЮЧОМ (event_id / click_id).
--     В ней допустимы parse_errors по НЕключевым полям — строка остаётся, но помечена.
--   - Таблица ods.*_errors    — копия строк с ЛЮБОЙ ошибкой парсинга (для разбора).
--   Поэтому строка с валидным ключом, но битым неключевым полем, попадёт И туда, И туда.
--
-- Когда запускать:
--   - В DAG etl_pipeline перед ODS → DDS
--   - После загрузки очередного среза данных в Kafka/STG
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Подготовка: очищаем ODS перед полной пересборкой
-- ----------------------------------------------------------------------------
TRUNCATE TABLE ods.browser_event;
TRUNCATE TABLE ods.browser_event_errors;
TRUNCATE TABLE ods.location_event;
TRUNCATE TABLE ods.location_event_errors;
TRUNCATE TABLE ods.device_by_click;
TRUNCATE TABLE ods.device_by_click_errors;
TRUNCATE TABLE ods.geo_by_click;
TRUNCATE TABLE ods.geo_by_click_errors;

-- ============================================================================
-- BROWSER EVENTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные ключи event_id
-- ----------------------------------------------------------------------------
INSERT INTO ods.browser_event
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
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', ''),
        if(event_ts IS NULL, 'bad_event_timestamp', ''),
        if(click_id IS NULL, 'bad_click_id', '')
    ]) AS parse_errors
FROM stg.browser_raw
WHERE event_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с ошибками парсинга browser
-- ----------------------------------------------------------------------------
INSERT INTO ods.browser_event_errors
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
WHERE length(parse_errors) > 0
    AND (
        event_id IS NULL
        OR event_ts IS NULL
        OR click_id IS NULL
    );

-- ============================================================================
-- LOCATION EVENTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные ключи event_id
-- ----------------------------------------------------------------------------
INSERT INTO ods.location_event
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

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с ошибками парсинга location
-- ----------------------------------------------------------------------------
INSERT INTO ods.location_event_errors
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

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные ключи click_id
-- ----------------------------------------------------------------------------
INSERT INTO ods.device_by_click
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

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с ошибками парсинга device
-- ----------------------------------------------------------------------------
INSERT INTO ods.device_by_click_errors
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

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные ключи click_id
-- ----------------------------------------------------------------------------
INSERT INTO ods.geo_by_click
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

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с ошибками парсинга geo
-- ----------------------------------------------------------------------------
INSERT INTO ods.geo_by_click_errors
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
