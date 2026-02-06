-- ============================================================================
-- Слой DDS (Detailed Data Store) — детальные сущности для аналитики
-- ============================================================================
-- Назначение:
--   - Собранные "чистые" сущности из ODS для JOIN'ов и аналитики
--   - click: объединяет device + geo (контекст сессии пользователя)
--   - event: объединяет browser + location (контекст события)
--
-- Загрузка:
--   Batch SQL (не MV!) — для согласованности при late arrivals
--   См. jobs/30_dds_refresh.sql
--
-- Почему не MV:
--   - MV с JOIN даёт eventual consistency (данные приходят в разное время)
--   - Batch позволяет сделать снапшот через argMax и корректно джойнить
--   - Контроль: можно проверить SQL, откатить, перезапустить
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Сущность: dds.click (контекст сессии пользователя)
-- ----------------------------------------------------------------------------
-- Объединяет данные из ods.device_by_click + ods.geo_by_click
-- Связь с event через click_id (LEFT JOIN, не все события имеют device/geo)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.click
(
    click_id         UUID,                          -- UUID сессии/клика (PK)
    user_domain_id   Nullable(UUID),                -- UUID пользователя в системе
    user_custom_id   Nullable(String),              -- Email/username
    device_type      LowCardinality(Nullable(String)),  -- Mobile, Computer, Tablet
    device_is_mobile Nullable(UInt8),               -- 1 = мобильное, 0 = десктоп
    os_name          LowCardinality(Nullable(String)),  -- Windows, iOS, Android и т.д.
    os               Nullable(String),              -- Полное название ОС
    os_timezone      LowCardinality(Nullable(String)),  -- Таймзона пользователя
    geo_country      LowCardinality(Nullable(String)),  -- Код страны (RU, US)
    geo_region_name  Nullable(String),              -- Регион/город
    geo_timezone     LowCardinality(Nullable(String)),  -- Таймзона по гео
    geo_latitude     Nullable(Float64),             -- Широта
    geo_longitude    Nullable(Float64),             -- Долгота
    ip_address       Nullable(String),              -- IP адрес
    dds_update_ts    DateTime64(3),                 -- Время загрузки в DDS (версия)
    ods_parse_errors Array(LowCardinality(String))  -- Ошибки из ODS + метки о пропусках
)
ENGINE = ReplacingMergeTree(dds_update_ts)          -- Дедупликация по версии
PARTITION BY toYYYYMM(toDate(dds_update_ts))        -- Партиция по месяцу загрузки
ORDER BY (click_id)                                 -- Ключ сортировки
SETTINGS allow_nullable_key = 1;                    -- Разрешаем NULL (на всякий случай)

-- ----------------------------------------------------------------------------
-- Сущность: dds.event (контекст события)
-- ----------------------------------------------------------------------------
-- Объединяет данные из ods.browser_event + ods.location_event
-- Связь с click через click_id (может быть NULL, если нет device/geo)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.event
(
    event_id           UUID,                        -- UUID события (PK)
    event_ts           Nullable(DateTime64(6)),     -- Время события
    event_date         Date MATERIALIZED ifNull(toDate(event_ts), toDate(dds_update_ts)),  -- Дата для партиций
    event_type         LowCardinality(Nullable(String)),  -- pageview, click, purchase и т.д.
    click_id           Nullable(UUID),              -- Связь с dds.click (может быть NULL)
    page_url           Nullable(String),            -- Полный URL
    page_url_path      LowCardinality(Nullable(String)),  -- Путь (/home, /product)
    referer_url        Nullable(String),            -- Откуда пришёл
    referer_medium     LowCardinality(Nullable(String)),  -- Тип referer
    utm_medium         LowCardinality(Nullable(String)),  -- UTM medium
    utm_source         LowCardinality(Nullable(String)),  -- UTM source
    utm_content        LowCardinality(Nullable(String)),  -- UTM content
    utm_campaign       LowCardinality(Nullable(String)),  -- UTM campaign
    browser_name       LowCardinality(Nullable(String)),  -- Chrome, Firefox
    browser_user_agent Nullable(String),            -- User-Agent
    browser_language   LowCardinality(Nullable(String)),  -- Язык браузера
    dds_update_ts      DateTime64(3),               -- Время загрузки в DDS (версия)
    ods_parse_errors   Array(LowCardinality(String))  -- Ошибки из ODS
)
ENGINE = ReplacingMergeTree(dds_update_ts)          -- Дедупликация по версии
PARTITION BY toYYYYMM(event_date)                   -- Партиция по дате события (важно для фильтров)
ORDER BY (event_id)                                 -- Ключ сортировки
SETTINGS allow_nullable_key = 1;                    -- Разрешаем NULL
