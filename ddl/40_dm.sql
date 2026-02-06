-- DM layer: Data Marts for BI (Superset)
-- Views for enriched data and pre-computed aggregations

-- Main enriched view: event + click context
CREATE VIEW IF NOT EXISTS dm.v_events_enriched AS
SELECT
    e.event_id,
    e.event_ts,
    e.event_date,
    e.event_type,
    e.click_id,
    e.page_url,
    e.page_url_path,
    e.referer_url,
    e.referer_medium,
    e.utm_medium,
    e.utm_source,
    e.utm_content,
    e.utm_campaign,
    e.browser_name,
    e.browser_language,
    e.browser_user_agent,
    c.user_domain_id,
    c.user_custom_id,
    c.device_type,
    c.device_is_mobile,
    c.os_name,
    c.os_timezone,
    c.geo_country,
    c.geo_region_name,
    c.geo_timezone,
    c.geo_latitude,
    c.geo_longitude,
    c.ip_address,
    e.dds_update_ts,
    arrayConcat(e.ods_parse_errors, c.ods_parse_errors) AS parse_errors
FROM dds.event AS e
LEFT JOIN dds.click AS c ON c.click_id = e.click_id;

-- Daily traffic aggregation
CREATE VIEW IF NOT EXISTS dm.v_daily_traffic AS
SELECT
    event_date,
    geo_country,
    device_type,
    browser_name,
    utm_source,
    utm_medium,
    count() AS events,
    uniqExact(click_id) AS uniq_clicks,
    uniqExact(user_domain_id) AS uniq_users
FROM dm.v_events_enriched
WHERE event_ts IS NOT NULL
GROUP BY
    event_date,
    geo_country,
    device_type,
    browser_name,
    utm_source,
    utm_medium;

-- Top pages daily
CREATE VIEW IF NOT EXISTS dm.v_top_pages_daily AS
SELECT
    event_date,
    page_url_path,
    count() AS pageviews,
    uniqExact(click_id) AS uniq_clicks
FROM dm.v_events_enriched
WHERE event_type = 'pageview'
GROUP BY event_date, page_url_path;

-- Data Quality errors daily
CREATE VIEW IF NOT EXISTS dm.v_dq_errors_daily AS
SELECT
    event_date,
    arrayJoin(parse_errors) AS error_code,
    count() AS rows_cnt
FROM dm.v_events_enriched
WHERE length(parse_errors) > 0
GROUP BY event_date, error_code;

-- User sessions overview (approximate, by click_id within 30min windows)
CREATE VIEW IF NOT EXISTS dm.v_session_overview AS
SELECT
    event_date,
    user_domain_id,
    click_id,
    min(event_ts) AS session_start,
    max(event_ts) AS session_end,
    date_diff('minute', min(event_ts), max(event_ts)) AS session_duration_min,
    count() AS events_count,
    arrayDistinct(groupArray(page_url_path)) AS pages_visited,
    arrayDistinct(groupArray(geo_country)) AS countries,
    arrayDistinct(groupArray(device_type)) AS devices,
    groupArraySample(1, 1919)(utm_source)[1] AS utm_source_last,
    groupArraySample(1, 1919)(utm_medium)[1] AS utm_medium_last
FROM dm.v_events_enriched
WHERE user_domain_id IS NOT NULL
GROUP BY event_date, user_domain_id, click_id;

-- UTM effectiveness (for marketing analysis)
CREATE VIEW IF NOT EXISTS dm.v_utm_effectiveness AS
SELECT
    event_date,
    utm_source,
    utm_medium,
    utm_campaign,
    count() AS clicks,
    uniqExact(user_domain_id) AS uniq_users,
    uniqExact(click_id) AS uniq_sessions,
    countIf(event_type = 'pageview') AS pageviews,
    countIf(event_type = 'purchase') AS purchases,
    countIf(event_type = 'add_to_cart') AS add_to_carts
FROM dm.v_events_enriched
WHERE utm_source IS NOT NULL OR utm_medium IS NOT NULL
GROUP BY event_date, utm_source, utm_medium, utm_campaign;
