-- ============================================================================
-- Слой STG (Staging) — сырые JSON + интеграция с Kafka
-- ============================================================================
-- Назначение:
--   - Хранение сырых данных "как есть" из Kafka
--   - Метаданные доставки (topic, partition, offset, timestamp)
--   - Источник для отладки и восстановления
--
-- Поток данных:
--   Kafka → kafka_*_raw (ENGINE=Kafka) → MV → *_raw (MergeTree)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблицы для сырых данных (MergeTree)
-- ----------------------------------------------------------------------------
-- Хранят JSON "как есть" + метаданные Kafka
-- Партиционируем по месяцу ingest_ts для эффективной очистки старых данных
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.browser_raw
(
    ingest_ts       DateTime64(3) DEFAULT now64(3),     -- Время вставки в ClickHouse
    kafka_topic     LowCardinality(String) DEFAULT '',  -- Топик Kafka
    kafka_partition Int32 DEFAULT -1,                   -- Партиция
    kafka_offset    Int64 DEFAULT -1,                   -- Смещение (гарантирует уникальность)
    kafka_ts        DateTime64(3) DEFAULT ingest_ts,    -- Время из Kafka (если есть)
    raw             String                              -- JSON как строка
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (kafka_topic, kafka_partition, kafka_offset, ingest_ts);

CREATE TABLE IF NOT EXISTS stg.location_raw
(
    ingest_ts       DateTime64(3) DEFAULT now64(3),
    kafka_topic     LowCardinality(String) DEFAULT '',
    kafka_partition Int32 DEFAULT -1,
    kafka_offset    Int64 DEFAULT -1,
    kafka_ts        DateTime64(3) DEFAULT ingest_ts,
    raw             String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (kafka_topic, kafka_partition, kafka_offset, ingest_ts);

CREATE TABLE IF NOT EXISTS stg.device_raw
(
    ingest_ts       DateTime64(3) DEFAULT now64(3),
    kafka_topic     LowCardinality(String) DEFAULT '',
    kafka_partition Int32 DEFAULT -1,
    kafka_offset    Int64 DEFAULT -1,
    kafka_ts        DateTime64(3) DEFAULT ingest_ts,
    raw             String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (kafka_topic, kafka_partition, kafka_offset, ingest_ts);

CREATE TABLE IF NOT EXISTS stg.geo_raw
(
    ingest_ts       DateTime64(3) DEFAULT now64(3),
    kafka_topic     LowCardinality(String) DEFAULT '',
    kafka_partition Int32 DEFAULT -1,
    kafka_offset    Int64 DEFAULT -1,
    kafka_ts        DateTime64(3) DEFAULT ingest_ts,
    raw             String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (kafka_topic, kafka_partition, kafka_offset, ingest_ts);

-- ----------------------------------------------------------------------------
-- Таблицы-источники Kafka (ENGINE = Kafka)
-- ----------------------------------------------------------------------------
-- Читают данные из топиков Kafka в реальном времени
-- Не хранят данные постоянно, а "потребляют" их при SELECT/MV
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.kafka_browser_raw (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',      -- Адрес брокера (внутри Docker-сети)
    kafka_topic_list = 'browser_events',    -- Имя топика
    kafka_group_name = 'ch_stg_browser',    -- Группа консьюмеров (для контроля offset'ов)
    kafka_format = 'JSONAsString',          -- Читаем весь JSON как строку
    kafka_num_consumers = 1,                -- Количество консьюмеров (для параллелизма)
    kafka_handle_error_mode = 'stream';     -- Ошибки не прерывают чтение

CREATE TABLE IF NOT EXISTS stg.kafka_location_raw (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'location_events',
    kafka_group_name = 'ch_stg_location',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS stg.kafka_device_raw (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'device_events',
    kafka_group_name = 'ch_stg_device',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS stg.kafka_geo_raw (raw String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'geo_events',
    kafka_group_name = 'ch_stg_geo',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- ----------------------------------------------------------------------------
-- Materialized Views: Kafka → STG
-- ----------------------------------------------------------------------------
-- Автоматически перекладывают данные из Kafka-таблиц в MergeTree
-- Работают в реальном времени: сообщение из Kafka → сразу в *_raw
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_kafka_browser_to_stg
TO stg.browser_raw
AS
SELECT
    now64(3) AS ingest_ts,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    fromUnixTimestamp64Milli(toInt64(_timestamp_ms)) AS kafka_ts,
    raw
FROM stg.kafka_browser_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_kafka_location_to_stg
TO stg.location_raw
AS
SELECT
    now64(3) AS ingest_ts,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    fromUnixTimestamp64Milli(toInt64(_timestamp_ms)) AS kafka_ts,
    raw
FROM stg.kafka_location_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_kafka_device_to_stg
TO stg.device_raw
AS
SELECT
    now64(3) AS ingest_ts,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    fromUnixTimestamp64Milli(toInt64(_timestamp_ms)) AS kafka_ts,
    raw
FROM stg.kafka_device_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_kafka_geo_to_stg
TO stg.geo_raw
AS
SELECT
    now64(3) AS ingest_ts,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    fromUnixTimestamp64Milli(toInt64(_timestamp_ms)) AS kafka_ts,
    raw
FROM stg.kafka_geo_raw;
