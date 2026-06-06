-- ============================================================================
-- Слой ODS (Operational Data Store) — типизированные данные + дедупликация + DQ
-- ============================================================================
-- Назначение:
--   - Хранение типизированных данных для дальнейшей сборки DDS
--   - Дедупликация через ReplacingMergeTree (последняя версия по src_ingest_ts)
--   - Контроль качества: массив parse_errors для "грязных" данных
--   - Разделение: валидные строки → основная таблица, ошибки → *_errors
--
-- Важно:
--   - Наполнение ODS выполняется batch-процессом из sql/ods/20_stg_to_ods.sql
--     (а не Materialized View, как в STG): батч ради наблюдаемости пересчёта
-- ============================================================================

-- ============================================================================
-- BROWSER EVENTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Основная таблица: валидные строки (event_id IS NOT NULL)
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
ENGINE = ReplacingMergeTree(src_ingest_ts)
-- Партиционируем по бизнес-дате события: у browser_event есть собственное время (event_ts).
-- У click-контекста ниже (location/device/geo) такого времени нет — там партиция по дате загрузки.
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_id)
SETTINGS allow_nullable_key = 1;

-- ----------------------------------------------------------------------------
-- Таблица ошибок: строки с невалидными ключами/критичными ошибками browser
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods.browser_event_errors
(
    ingest_ts       DateTime64(3),            -- Время вставки в ClickHouse
    kafka_topic     LowCardinality(String),   -- Топик Kafka (для отслеживания источника)
    kafka_partition Int32,                    -- Партиция Kafka
    kafka_offset    Int64,                    -- Смещение Kafka (идентификатор сообщения)
    kafka_ts        DateTime64(3),            -- Время из Kafka
    raw             String,                   -- Исходный JSON
    error_reason    LowCardinality(String)    -- Описание ошибки
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, kafka_topic, kafka_partition, kafka_offset);

-- ============================================================================
-- LOCATION EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.location_event
(
    event_id       Nullable(UUID),
    page_url       Nullable(String),                  -- Полный URL страницы
    page_url_path  LowCardinality(Nullable(String)),  -- Путь (/home, /product и т.д.)
    referer_url    Nullable(String),                  -- Откуда пришёл пользователь
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
-- По дате загрузки: у location нет собственного времени события (только время приёма в STG)
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

-- ============================================================================
-- DEVICE EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.device_by_click
(
    click_id         Nullable(UUID),
    os               Nullable(String),                  -- Полное название ОС
    os_name          LowCardinality(Nullable(String)),  -- Короткое название (Windows, iOS и т.д.)
    os_timezone      LowCardinality(Nullable(String)),  -- Таймзона пользователя
    device_type      LowCardinality(Nullable(String)),  -- Mobile, Computer, Tablet
    device_is_mobile Nullable(UInt8),                   -- 1 = мобильное, 0 = десктоп
    user_custom_id   Nullable(String),                  -- Email или username
    user_domain_id   Nullable(UUID),                    -- UUID пользователя в системе
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

-- ============================================================================
-- GEO EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.geo_by_click
(
    click_id        Nullable(UUID),
    geo_latitude    Nullable(Float64),                  -- Широта
    geo_longitude   Nullable(Float64),                  -- Долгота
    geo_country     LowCardinality(Nullable(String)),   -- Код страны (RU, US и т.д.)
    geo_timezone    LowCardinality(Nullable(String)),   -- Таймзона
    geo_region_name Nullable(String),                   -- Название региона/города
    ip_address      Nullable(String),                   -- IP адрес
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
