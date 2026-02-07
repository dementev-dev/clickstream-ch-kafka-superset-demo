-- ============================================================================
-- Создание баз данных для слоёв хранилища
-- ============================================================================
-- stg — Staging: сырые данные из Kafka
-- ods — Operational Data Store: типизированные данные с DQ
-- dds — Detailed Data Store: детальные сущности для аналитики
-- dm  — Data Marts: витрины для BI
-- ============================================================================

CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dds;
CREATE DATABASE IF NOT EXISTS dm;
