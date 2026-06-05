-- ============================================================================
-- Batch-трансформация: ODS → DDS
-- ============================================================================
-- Поток данных:
--   ods.device_by_click + ods.geo_by_click   → dds.click  (контекст клика)
--   ods.browser_event   + ods.location_event → dds.event  (контекст события)
--
-- Что делает:
--   Собирает "чистые" сущности из типизированных данных ODS для аналитики.
--   Использует argMax() для получения последней версии строк по ключу.
--
-- Когда запускать:
--   - Периодически (например, каждые N минут)
--   - Через Airflow по расписанию
--   - Вручную после загрузки новых данных
--
-- Стратегия:
--   Сейчас: полная перезагрузка (TRUNCATE + INSERT) — проще для демо
--   В продакшене: инкрементальная загрузка по watermark (src_ingest_ts)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Сущность: dds.click (объединяет device + geo)
-- ----------------------------------------------------------------------------
-- Проблема: device и geo события приходят независимо (не все click_id есть в обоих источниках)
-- Решение: UNION всех click_id + LEFT JOIN (обрабатываем geo-only и device-only)
-- ----------------------------------------------------------------------------
INSERT INTO dds.click
SELECT
    c.click_id,
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
    now64(3) AS dds_update_ts,  -- Время загрузки в DDS для версионирования
    -- Собираем все ошибки парсинга из источников + отслеживаем пропущенные данные
    arrayFilter(x -> x != '', arrayConcat(
        ifNull(d.parse_errors, []),
        if(d.click_id IS NULL, ['device_not_found'], []),      -- Нет данных об устройстве
        if(g.click_id IS NULL, ['geo_not_found'], []),          -- Нет гео-данных
        if(g.geo_country IS NULL, ['geo_country_missing'], [])  -- Гео есть, но страна не определена
    )) AS ods_parse_errors
FROM (
    -- Шаг 1: Собираем ВСЕ уникальные click_id из обоих источников
    -- Это позволяет обработать случаи, когда:
    --   - есть device, но нет geo (geo-only клики)
    --   - есть geo, но нет device (device-only клики)
    SELECT click_id
    FROM (
        SELECT assumeNotNull(click_id) AS click_id
        FROM ods.device_by_click
        WHERE click_id IS NOT NULL
        GROUP BY click_id
    )
    UNION DISTINCT
    SELECT click_id
    FROM (
        SELECT assumeNotNull(click_id) AS click_id
        FROM ods.geo_by_click
        WHERE click_id IS NOT NULL
        GROUP BY click_id
    )
) AS c
-- Шаг 2: LEFT JOIN с device (может не быть данных)
LEFT JOIN (
    -- Снапшот device: последняя версия каждого click_id по времени загрузки
    SELECT
        assumeNotNull(click_id) AS click_id,
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
) AS d ON d.click_id = c.click_id
-- Шаг 3: LEFT JOIN с geo (может не быть данных)
LEFT JOIN (
    -- Снапшот geo: последняя версия каждого click_id по времени загрузки
    SELECT
        assumeNotNull(click_id) AS click_id,
        argMax(geo_country, src_ingest_ts) AS geo_country,
        argMax(geo_region_name, src_ingest_ts) AS geo_region_name,
        argMax(geo_timezone, src_ingest_ts) AS geo_timezone,
        argMax(geo_latitude, src_ingest_ts) AS geo_latitude,
        argMax(geo_longitude, src_ingest_ts) AS geo_longitude,
        argMax(ip_address, src_ingest_ts) AS ip_address
    FROM ods.geo_by_click
    WHERE click_id IS NOT NULL
    GROUP BY click_id
) AS g ON g.click_id = c.click_id;

-- ----------------------------------------------------------------------------
-- Сущность: dds.event (объединяет browser + location)
-- ----------------------------------------------------------------------------
-- Логика проще: event_id связывает browser и location 1:1
-- Если location нет — это тоже полезная информация (ошибка или пропуск)
-- ----------------------------------------------------------------------------
INSERT INTO dds.event
SELECT
    b.event_id,
    b.event_ts,
    b.event_type,
    b.click_id,
    -- Поля из location (могут быть NULL, если location не пришёл)
    l.page_url,
    l.page_url_path,
    l.referer_url,
    l.referer_medium,
    l.utm_medium,
    l.utm_source,
    l.utm_content,
    l.utm_campaign,
    -- Поля из browser
    b.browser_name,
    b.browser_user_agent,
    b.browser_language,
    now64(3) AS dds_update_ts,
    -- Собираем ошибки парсинга + отмечаем, если location не найден
    arrayFilter(x -> x != '', arrayConcat(
        b.parse_errors,
        if(l.event_id IS NULL, ['location_not_found'], [])
    )) AS ods_parse_errors
FROM (
    -- Снапшот browser: последняя версия каждого event_id
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
    WHERE event_id IS NOT NULL  -- Фильтруем битые ключи (они в error_tables)
    GROUP BY event_id
) AS b
-- LEFT JOIN с location (может не быть данных для некоторых событий)
LEFT JOIN (
    -- Снапшот location: последняя версия каждого event_id
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
