-- Batch transformation: DDS → DM materialized tables
-- For demo we use VIEWs mainly, but here we can materialize heavy aggregations

-- Materialized daily traffic (if needed for performance)
-- Uncomment if VIEW dm.v_daily_traffic becomes too slow
/*
CREATE TABLE IF NOT EXISTS dm.daily_traffic_mart
(
    event_date Date,
    geo_country LowCardinality(Nullable(String)),
    device_type LowCardinality(Nullable(String)),
    browser_name LowCardinality(Nullable(String)),
    utm_source LowCardinality(Nullable(String)),
    utm_medium LowCardinality(Nullable(String)),
    events UInt64,
    uniq_clicks UInt64,
    uniq_users UInt64
)
ENGINE = ReplacingMergeTree(event_date)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, geo_country, device_type, browser_name, utm_source, utm_medium);

TRUNCATE TABLE dm.daily_traffic_mart;

INSERT INTO dm.daily_traffic_mart
SELECT * FROM dm.v_daily_traffic;
*/

-- Data Quality summary table (always fresh)
CREATE TABLE IF NOT EXISTS dm.dq_summary
(
    check_date Date,
    layer LowCardinality(String),
    table_name LowCardinality(String),
    check_name LowCardinality(String),
    check_value UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(check_date)
ORDER BY (check_date, layer, table_name, check_name);

-- Truncate and refill DQ summary
INSERT INTO dm.dq_summary
SELECT
    today() AS check_date,
    'stg' AS layer,
    'browser_raw' AS table_name,
    'total_rows' AS check_name,
    count() AS check_value
FROM stg.browser_raw
UNION ALL
SELECT today(), 'stg', 'location_raw', 'total_rows', count() FROM stg.location_raw
UNION ALL
SELECT today(), 'stg', 'device_raw', 'total_rows', count() FROM stg.device_raw
UNION ALL
SELECT today(), 'stg', 'geo_raw', 'total_rows', count() FROM stg.geo_raw
UNION ALL
SELECT today(), 'ods', 'browser_event', 'total_rows', count() FROM ods.browser_event
UNION ALL
SELECT today(), 'ods', 'browser_event', 'rows_with_errors', count() FROM ods.browser_event WHERE length(parse_errors) > 0
UNION ALL
SELECT today(), 'ods', 'location_event', 'total_rows', count() FROM ods.location_event
UNION ALL
SELECT today(), 'ods', 'device_by_click', 'total_rows', count() FROM ods.device_by_click
UNION ALL
SELECT today(), 'ods', 'geo_by_click', 'total_rows', count() FROM ods.geo_by_click
UNION ALL
SELECT today(), 'dds', 'event', 'total_rows', count() FROM dds.event
UNION ALL
SELECT today(), 'dds', 'click', 'total_rows', count() FROM dds.click
UNION ALL
SELECT today(), 'dds', 'event_without_click', 'orphan_events', count() FROM dds.event WHERE click_id IS NOT NULL AND click_id NOT IN (SELECT click_id FROM dds.click);
