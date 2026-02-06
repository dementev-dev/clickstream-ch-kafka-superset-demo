-- DDS layer: detailed entities (event + click context)
-- Populated via batch SQL (not MV) to handle late arrivals and ensure consistency

-- DDS: click context (device + geo)
CREATE TABLE IF NOT EXISTS dds.click
(
    click_id         UUID,
    user_domain_id   Nullable(UUID),
    user_custom_id   Nullable(String),
    device_type      LowCardinality(Nullable(String)),
    device_is_mobile Nullable(UInt8),
    os_name          LowCardinality(Nullable(String)),
    os               Nullable(String),
    os_timezone      LowCardinality(Nullable(String)),
    geo_country      LowCardinality(Nullable(String)),
    geo_region_name  Nullable(String),
    geo_timezone     LowCardinality(Nullable(String)),
    geo_latitude     Nullable(Float64),
    geo_longitude    Nullable(Float64),
    ip_address       Nullable(String),
    dds_update_ts    DateTime64(3),
    ods_parse_errors Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(dds_update_ts)
PARTITION BY toYYYYMM(toDate(dds_update_ts))
ORDER BY (click_id)
SETTINGS allow_nullable_key = 1;

-- DDS: event (browser + location)
CREATE TABLE IF NOT EXISTS dds.event
(
    event_id           UUID,
    event_ts           Nullable(DateTime64(6)),
    event_date         Date MATERIALIZED ifNull(toDate(event_ts), toDate(dds_update_ts)),
    event_type         LowCardinality(Nullable(String)),
    click_id           Nullable(UUID),
    page_url           Nullable(String),
    page_url_path      LowCardinality(Nullable(String)),
    referer_url        Nullable(String),
    referer_medium     LowCardinality(Nullable(String)),
    utm_medium         LowCardinality(Nullable(String)),
    utm_source         LowCardinality(Nullable(String)),
    utm_content        LowCardinality(Nullable(String)),
    utm_campaign       LowCardinality(Nullable(String)),
    browser_name       LowCardinality(Nullable(String)),
    browser_user_agent Nullable(String),
    browser_language   LowCardinality(Nullable(String)),
    dds_update_ts      DateTime64(3),
    ods_parse_errors   Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(dds_update_ts)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_id)
SETTINGS allow_nullable_key = 1;
