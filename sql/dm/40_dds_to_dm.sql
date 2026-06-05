-- ============================================================================
-- Batch-трансформация: DDS → DM (Data Quality summary)
-- ============================================================================
-- Поток данных:
--   dds.click + dds.event + ods.* + stg.* → dm.dq_summary  (сводка по слоям)
--   Сами витрины (dm.v_*) — это VIEW поверх DDS, создаются в sql/ddl/dm/40_dm.sql.
--
-- Что делает:
--   Собирает статистику по всем слоям (stg/ods/dds) для мониторинга качества данных.
--   Позволяет быстро проверить, сколько данных прошло через каждый слой
--   и сколько ошибок было на каждом этапе.
--
-- Важно:
--   Таблица dq_summary пересоздаётся при каждом запуске (TRUNCATE + INSERT),
--   чтобы не накапливать дубликаты при повторных прогонах.
--
-- Витрины DM сейчас — это VIEW (логика без копии данных). Если тяжёлая агрегация
-- начнёт тормозить, её материализуют в таблицу — пример в docs/ARCHITECTURE.md,
-- раздел «Материализация витрин».
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица для сводки по качеству данных (DQ summary)
-- ----------------------------------------------------------------------------
-- Хранит метрики по всем слоям для быстрой проверки пайплайна
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.dq_summary
(
    check_date Date,                    -- Дата проверки
    layer LowCardinality(String),       -- Слой: stg, ods, dds
    table_name LowCardinality(String),  -- Имя таблицы
    check_name LowCardinality(String),  -- Тип проверки: total_rows, rows_with_errors и т.д.
    check_value UInt64                  -- Значение метрики
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(check_date)
ORDER BY (check_date, layer, table_name, check_name);

-- Очищаем перед заполнением, чтобы не было дубликатов при повторных запусках
TRUNCATE TABLE dm.dq_summary;

-- ----------------------------------------------------------------------------
-- Заполняем сводку метриками по всем слоям
-- ----------------------------------------------------------------------------
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
-- Считаем строки с ошибками парсинга в ODS
SELECT today(), 'ods', 'browser_event', 'rows_with_errors', count() 
FROM ods.browser_event 
WHERE length(parse_errors) > 0

UNION ALL
SELECT today(), 'ods', 'location_event', 'total_rows', count() FROM ods.location_event
UNION ALL
SELECT today(), 'ods', 'location_event', 'rows_with_errors', count()
FROM ods.location_event
WHERE length(parse_errors) > 0

UNION ALL
SELECT today(), 'ods', 'device_by_click', 'total_rows', count() FROM ods.device_by_click
UNION ALL
SELECT today(), 'ods', 'device_by_click', 'rows_with_errors', count()
FROM ods.device_by_click
WHERE length(parse_errors) > 0

UNION ALL
SELECT today(), 'ods', 'geo_by_click', 'total_rows', count() FROM ods.geo_by_click
UNION ALL
SELECT today(), 'ods', 'geo_by_click', 'rows_with_errors', count()
FROM ods.geo_by_click
WHERE length(parse_errors) > 0

UNION ALL
SELECT today(), 'ods', 'browser_event_errors', 'total_rows', count() FROM ods.browser_event_errors
UNION ALL
SELECT today(), 'ods', 'location_event_errors', 'total_rows', count() FROM ods.location_event_errors
UNION ALL
SELECT today(), 'ods', 'device_by_click_errors', 'total_rows', count() FROM ods.device_by_click_errors
UNION ALL
SELECT today(), 'ods', 'geo_by_click_errors', 'total_rows', count() FROM ods.geo_by_click_errors

UNION ALL
SELECT today(), 'dds', 'event', 'total_rows', count() FROM dds.event
UNION ALL
SELECT today(), 'dds', 'click', 'total_rows', count() FROM dds.click
UNION ALL
-- Считаем "осиротевшие" события (есть click_id, но нет такого click в dds.click)
SELECT today(), 'dds', 'event_without_click', 'orphan_events', count() 
FROM dds.event 
WHERE click_id IS NOT NULL 
  AND click_id NOT IN (SELECT click_id FROM dds.click);
