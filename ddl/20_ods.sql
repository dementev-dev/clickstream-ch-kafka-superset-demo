-- ODS layer: typed data + deduplication + DQ

-- Main ODS tables (valid keys only) + error tables (invalid keys)

-- ODS: browser_events
CREATE TABLE IF NOT EXISTS ods.browser_event
(
    event_id           Nullable(UUID),
    event_ts           Nullable(DateTime64(6)),
    event_date         Date MATERIALIZED ifNull(toDate(event_ts), toDate(src_ingest_ts)),
    event_type         LowCardinality(Nullable(String)),
    click_id           Nullable(UUID),
    browser_name       LowCardinality(Nullable(String)),
    browser_user_agent Nullable(String),
    browser_language   LowCardinality(Nullable(String)),
    src_ingest_ts      DateTime64(3),
    src_raw            String,
    parse_errors       Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS ods.browser_event_errors
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
    arrayFilter(x -> x != '', [
        if(event_id IS NULL, 'bad_event_id', ''),
        if(event_ts IS NULL, 'bad_event_timestamp', ''),
        if(click_id IS NULL, 'bad_click_id', '')
    ]) AS parse_errors
FROM stg.browser_raw
WHERE event_id IS NOT NULL;  -- Filter NULL keys to error table

-- ODS: location_events
CREATE TABLE IF NOT EXISTS ods.location_event
(
    event_id       Nullable(UUID),
    page_url       Nullable(String),
    page_url_path  LowCardinality(Nullable(String)),
    referer_url    Nullable(String),
    referer_medium LowCardinality(Nullable(String)),
    utm_medium     LowCardinality(Nullable(String)),
    utm_source     LowCardinality(Nullable(String)),
    utm_content    LowCardinality(Nullable(String)),
    utm_campaign   LowCardinality(Nullable(String)),
    src_ingest_ts  DateTime64(3),
    src_raw        String,
    parse_errors   Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(src_ingest_ts)
PARTITION BY toYYYYMM(toDate(src_ingest_ts))
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

-- ODS: device_events
CREATE TABLE IF NOT EXISTS ods.device_by_click
(
    click_id         Nullable(UUID),
    os               Nullable(String),
    os_name          LowCardinality(Nullable(String)),
    os_timezone      LowCardinality(Nullable(String)),
    device_type      LowCardinality(Nullable(String)),
    device_is_mobile Nullable(UInt8),
    user_custom_id   Nullable(String),
    user_domain_id   Nullable(UUID),
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

-- ODS: geo_events
CREATE TABLE IF NOT EXISTS ods.geo_by_click
(
    click_id        Nullable(UUID),
    geo_latitude    Nullable(Float64),
    geo_longitude   Nullable(Float64),
    geo_country     LowCardinality(Nullable(String)),
    geo_timezone    LowCardinality(Nullable(String)),
    geo_region_name Nullable(String),
    ip_address      Nullable(String),
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
