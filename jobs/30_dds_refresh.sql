-- Batch transformation: ODS → DDS
-- Gets latest version from ODS (using argMax) and builds DDS entities
-- Should be run periodically (e.g., every N minutes or via Airflow)

-- Refresh DDS.click (from device + geo)
-- Strategy: full rebuild for demo; incremental for production
INSERT INTO dds.click
SELECT
    d.click_id,
    d.user_domain_id,
    d.user_custom_id,
    d.device_type,
    d.device_is_mobile,
    d.os_name,
    d.os,
    d.os_timezone,
    g.geo_country,
    g.geo_region_name,
    g.geo_timezone,
    g.geo_latitude,
    g.geo_longitude,
    g.ip_address,
    now64(3) AS dds_update_ts,
    arrayFilter(x -> x != '', arrayConcat(
        d.parse_errors,
        if(g.click_id IS NULL, ['geo_not_found'], []),
        if(g.geo_country IS NULL, ['geo_country_missing'], [])
    )) AS ods_parse_errors
FROM (
    -- Latest device snapshot from ODS
    SELECT
        click_id,
        argMax(user_domain_id, src_ingest_ts) AS user_domain_id,
        argMax(user_custom_id, src_ingest_ts) AS user_custom_id,
        argMax(device_type, src_ingest_ts) AS device_type,
        argMax(device_is_mobile, src_ingest_ts) AS device_is_mobile,
        argMax(os_name, src_ingest_ts) AS os_name,
        argMax(os, src_ingest_ts) AS os,
        argMax(os_timezone, src_ingest_ts) AS os_timezone,
        argMax(parse_errors, src_ingest_ts) AS parse_errors
    FROM ods.device_by_click
    WHERE click_id IS NOT NULL
    GROUP BY click_id
) AS d
LEFT JOIN (
    -- Latest geo snapshot from ODS
    SELECT
        click_id,
        argMax(geo_country, src_ingest_ts) AS geo_country,
        argMax(geo_region_name, src_ingest_ts) AS geo_region_name,
        argMax(geo_timezone, src_ingest_ts) AS geo_timezone,
        argMax(geo_latitude, src_ingest_ts) AS geo_latitude,
        argMax(geo_longitude, src_ingest_ts) AS geo_longitude,
        argMax(ip_address, src_ingest_ts) AS ip_address
    FROM ods.geo_by_click
    WHERE click_id IS NOT NULL
    GROUP BY click_id
) AS g ON g.click_id = d.click_id;

-- Refresh DDS.event (from browser + location)
INSERT INTO dds.event
SELECT
    b.event_id,
    b.event_ts,
    b.event_type,
    b.click_id,
    l.page_url,
    l.page_url_path,
    l.referer_url,
    l.referer_medium,
    l.utm_medium,
    l.utm_source,
    l.utm_content,
    l.utm_campaign,
    b.browser_name,
    b.browser_user_agent,
    b.browser_language,
    now64(3) AS dds_update_ts,
    arrayFilter(x -> x != '', arrayConcat(
        b.parse_errors,
        if(l.event_id IS NULL, ['location_not_found'], [])
    )) AS ods_parse_errors
FROM (
    -- Latest browser snapshot from ODS
    SELECT
        event_id,
        argMax(event_ts, src_ingest_ts) AS event_ts,
        argMax(event_type, src_ingest_ts) AS event_type,
        argMax(click_id, src_ingest_ts) AS click_id,
        argMax(browser_name, src_ingest_ts) AS browser_name,
        argMax(browser_user_agent, src_ingest_ts) AS browser_user_agent,
        argMax(browser_language, src_ingest_ts) AS browser_language,
        argMax(parse_errors, src_ingest_ts) AS parse_errors
    FROM ods.browser_event
    WHERE event_id IS NOT NULL
    GROUP BY event_id
) AS b
LEFT JOIN (
    -- Latest location snapshot from ODS
    SELECT
        event_id,
        argMax(page_url, src_ingest_ts) AS page_url,
        argMax(page_url_path, src_ingest_ts) AS page_url_path,
        argMax(referer_url, src_ingest_ts) AS referer_url,
        argMax(referer_medium, src_ingest_ts) AS referer_medium,
        argMax(utm_medium, src_ingest_ts) AS utm_medium,
        argMax(utm_source, src_ingest_ts) AS utm_source,
        argMax(utm_content, src_ingest_ts) AS utm_content,
        argMax(utm_campaign, src_ingest_ts) AS utm_campaign
    FROM ods.location_event
    WHERE event_id IS NOT NULL
    GROUP BY event_id
) AS l ON l.event_id = b.event_id;
