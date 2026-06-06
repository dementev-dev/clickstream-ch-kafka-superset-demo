-- ============================================================================
-- Слой DM (Data Marts) — витрины для BI (Superset/Grafana)
-- ============================================================================
-- Назначение:
--   - Представления (VIEW) для удобного доступа к данным из BI-инструментов
--   - Обогащение: соединяем event + click через LEFT JOIN
--   - Агрегации: готовые GROUP BY для частых запросов
--
-- Почему VIEW:
--   - Гибкость: меняем логику без пересоздания таблиц
--   - Нет дублирования данных (храним только в DDS)
--   - Для демо: производительность достаточная
--
-- Для продакшена:
--   - Если тяжёлые агрегации тормозят — материализовать в таблицы
--   - См. пример закомментированный в sql/dm/40_dds_to_dm.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Витрина: полное обогащение событий (event + click)
-- ----------------------------------------------------------------------------
-- Соединяет dds.event и dds.click через click_id
-- LEFT JOIN: не все события имеют device/geo контекст
-- Используется как основа для других витрин
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_events_enriched AS
SELECT
    e.event_id,
    e.event_ts,
    e.event_date,
    e.event_type,
    e.click_id,
    -- Поля из location (через event)
    e.page_url,
    e.page_url_path,
    e.referer_url,
    e.referer_medium,
    e.utm_medium,
    e.utm_source,
    e.utm_content,
    e.utm_campaign,
    -- Поля из browser (через event)
    e.browser_name,
    e.browser_language,
    e.browser_user_agent,
    -- Поля из click (device + geo), могут быть NULL
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
    -- Технические поля
    e.dds_update_ts,
    -- Объединяем ошибки парсинга из обоих источников
    arrayConcat(e.ods_parse_errors, c.ods_parse_errors) AS parse_errors
FROM dds.event AS e
LEFT JOIN dds.click AS c ON c.click_id = e.click_id;

-- ----------------------------------------------------------------------------
-- Витрина: агрегация трафика по дням и измерениям
-- ----------------------------------------------------------------------------
-- Используется для анализа посещаемости
-- Гранулярность: дата × страна × устройство × браузер × UTM
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_daily_traffic AS
SELECT
    event_date,
    geo_country,
    device_type,
    browser_name,
    utm_source,
    utm_medium,
    count() AS events,                              -- Количество событий
    uniqExact(click_id) AS uniq_clicks,             -- Уникальные сессии
    uniqExact(user_domain_id) AS uniq_users         -- Уникальные пользователи
FROM dm.v_events_enriched
WHERE event_ts IS NOT NULL                          -- Фильтруем битые timestamp
GROUP BY
    event_date,
    geo_country,
    device_type,
    browser_name,
    utm_source,
    utm_medium;

-- ----------------------------------------------------------------------------
-- Витрина: популярность страниц (воронка)
-- ----------------------------------------------------------------------------
-- Показывает какие страницы чаще всего просматривают
-- Используется для анализа воронки конверсии
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_top_pages_daily AS
SELECT
    event_date,
    page_url_path,                                  -- Путь URL (/home, /product)
    count() AS pageviews,                           -- Количество просмотров
    uniqExact(click_id) AS uniq_clicks              -- Уникальные сессии
FROM dm.v_events_enriched
WHERE event_type = 'pageview'                       -- Только просмотры страниц
GROUP BY event_date, page_url_path;

-- ----------------------------------------------------------------------------
-- Витрина: ошибки парсинга по дням
-- ----------------------------------------------------------------------------
-- Для мониторинга качества данных
-- Показывает сколько строк с какими ошибками за каждый день
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_dq_errors_daily AS
SELECT
    event_date,
    arrayJoin(parse_errors) AS error_code,          -- Разворачиваем массив ошибок
    count() AS rows_cnt                             -- Количество строк с этой ошибкой
FROM dm.v_events_enriched
WHERE length(parse_errors) > 0                      -- Только строки с ошибками
GROUP BY event_date, error_code;

-- ----------------------------------------------------------------------------
-- Витрина: обзор сессий пользователей
-- ----------------------------------------------------------------------------
-- Группировка по click_id (сессия) и user_domain_id (пользователь)
-- Показывает длительность сессии, посещённые страницы, устройства
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_session_overview AS
SELECT
    event_date,
    user_domain_id,
    click_id,
    min(event_ts) AS session_start,                 -- Начало сессии
    max(event_ts) AS session_end,                   -- Конец сессии
    date_diff('minute', min(event_ts), max(event_ts)) AS session_duration_min,  -- Длительность
    count() AS events_count,                        -- Количество событий в сессии
    arrayDistinct(groupArray(page_url_path)) AS pages_visited,  -- Уникальные страницы
    arrayDistinct(groupArray(geo_country)) AS countries,        -- Страны (если менялась)
    arrayDistinct(groupArray(device_type)) AS devices,          -- Устройства (если менялось)
    -- Берём одну UTM-метку сессии (для атрибуции).
    -- groupArraySample(1, 1919) выбирает 1 случайный элемент; 1919 — фиксированный seed,
    -- чтобы выборка была воспроизводимой между прогонами (одинаковый seed → один результат).
    groupArraySample(1, 1919)(utm_source)[1] AS utm_source_last,
    groupArraySample(1, 1919)(utm_medium)[1] AS utm_medium_last
FROM dm.v_events_enriched
WHERE user_domain_id IS NOT NULL                    -- Только идентифицированные пользователи
GROUP BY event_date, user_domain_id, click_id;

-- ----------------------------------------------------------------------------
-- Витрина: эффективность UTM-кампаний (маркетинговая аналитика)
-- ----------------------------------------------------------------------------
-- Показывает какие каналы (utm_source/medium) приносят трафик
-- Отдельно считаем pageviews, purchases, add_to_carts
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dm.v_utm_effectiveness AS
SELECT
    event_date,
    utm_source,
    utm_medium,
    utm_campaign,
    count() AS clicks,                              -- Всего кликов/событий
    uniqExact(user_domain_id) AS uniq_users,        -- Уникальные пользователи
    uniqExact(click_id) AS uniq_sessions,           -- Уникальные сессии
    countIf(event_type = 'pageview') AS pageviews,  -- Только просмотры
    countIf(event_type = 'purchase') AS purchases,  -- Покупки (если есть)
    countIf(event_type = 'add_to_cart') AS add_to_carts  -- Добавления в корзину
FROM dm.v_events_enriched
WHERE utm_source IS NOT NULL OR utm_medium IS NOT NULL  -- Только с UTM-метками
GROUP BY event_date, utm_source, utm_medium, utm_campaign;
