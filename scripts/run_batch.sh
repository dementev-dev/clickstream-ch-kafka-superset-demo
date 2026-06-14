#!/usr/bin/env bash
#
# Скрипт batch-трансформации данных: STG → ODS → DDS → DM
#
# Назначение:
#   Запускает SQL-скрипты из sql/ods, sql/dds и sql/dm для преобразования данных между слоями:
#   1. STG → ODS : Типизация и перенос ошибок в *_errors
#   2. ODS → DDS : Сборка сущностей из типизированных данных
#   3. DDS → DM  : Обновление сводки по качеству данных (dq_summary)
#
# Как запускать:
#   make transform
#   или: bash scripts/run_batch.sh
#
# Требования:
#   - ClickHouse запущен (make up)
#   - STG содержит данные генератора или ручной загрузки
#
# Стратегия:
#   Сейчас: полная перезагрузка (TRUNCATE + INSERT) — для демо
#   В продакшене: инкрементальная загрузка по watermark
#

set -euo pipefail

# Директория со скриптом
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_ROOT_DIR="${SCRIPT_DIR}/../sql"
ODS_TRANSFORM_SQL="${SQL_ROOT_DIR}/ods/20_stg_to_ods.sql"
DDS_TRANSFORM_SQL="${SQL_ROOT_DIR}/dds/30_ods_to_dds.sql"
DM_TRANSFORM_SQL="${SQL_ROOT_DIR}/dm/40_dds_to_dm.sql"

# Параметры подключения
COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"

# -----------------------------------------------------------------------------
# Проверка: ClickHouse запущен?
# -----------------------------------------------------------------------------
if ! ${COMPOSE_BIN} ps | grep -q "${CLICKHOUSE_SERVICE}"; then
    echo "Ошибка: Сервис '${CLICKHOUSE_SERVICE}' не запущен."
    echo "Запустите сначала: make up"
    exit 1
fi

# -----------------------------------------------------------------------------
# Проверка: SQL-файлы batch существуют?
# -----------------------------------------------------------------------------
if [[ ! -f "${ODS_TRANSFORM_SQL}" ]]; then
    echo "Ошибка: Не найден SQL-файл: ${ODS_TRANSFORM_SQL}"
    exit 1
fi

if [[ ! -f "${DDS_TRANSFORM_SQL}" ]]; then
    echo "Ошибка: Не найден SQL-файл: ${DDS_TRANSFORM_SQL}"
    exit 1
fi

if [[ ! -f "${DM_TRANSFORM_SQL}" ]]; then
    echo "Ошибка: Не найден SQL-файл: ${DM_TRANSFORM_SQL}"
    exit 1
fi

# -----------------------------------------------------------------------------
# Проверка: в STG есть данные?
# -----------------------------------------------------------------------------
STG_COUNT=$(${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="
        SELECT
            (SELECT count() FROM stg.browser_raw)
            + (SELECT count() FROM stg.location_raw)
            + (SELECT count() FROM stg.device_raw)
            + (SELECT count() FROM stg.geo_raw) AS stg_rows_total
    " 2>/dev/null || echo "0")

if [[ "${STG_COUNT}" == "0" ]]; then
    echo "Предупреждение: Таблицы STG пусты."
    echo "Сначала загрузите данные: make generated-history-analytics"
    echo "Для ручной отладки можно выполнить backfill генератора или архивный make data."
    exit 1
fi

echo "Найдено ${STG_COUNT} строк в STG (суммарно по 4 потокам)"
echo ""

# -----------------------------------------------------------------------------
# Шаг 1: STG → ODS (типизация и DQ)
# -----------------------------------------------------------------------------
echo "Шаг 1: Обновление ODS слоя (STG → ODS)..."
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --multiquery < "${ODS_TRANSFORM_SQL}"

echo "  ✓ ODS обновлён"

# Показываем статистику ODS
echo ""
echo "Статистика ODS:"
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="
        SELECT 'ods.browser_event' AS table, count() AS rows FROM ods.browser_event
        UNION ALL SELECT 'ods.location_event', count() FROM ods.location_event
        UNION ALL SELECT 'ods.device_by_click', count() FROM ods.device_by_click
        UNION ALL SELECT 'ods.geo_by_click', count() FROM ods.geo_by_click
        UNION ALL SELECT 'ods.browser_event_errors', count() FROM ods.browser_event_errors
        UNION ALL SELECT 'ods.location_event_errors', count() FROM ods.location_event_errors
        UNION ALL SELECT 'ods.device_by_click_errors', count() FROM ods.device_by_click_errors
        UNION ALL SELECT 'ods.geo_by_click_errors', count() FROM ods.geo_by_click_errors
        FORMAT PrettyCompact
    "

# -----------------------------------------------------------------------------
# Шаг 2: ODS → DDS (сборка сущностей)
# -----------------------------------------------------------------------------
echo ""
echo "Шаг 2: Обновление DDS слоя (ODS → DDS)..."
echo "  - Очистка текущих данных (TRUNCATE)..."

# Очищаем таблицы перед загрузкой (полная перезагрузка для демо)
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="TRUNCATE TABLE dds.click" 2>/dev/null || true
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="TRUNCATE TABLE dds.event" 2>/dev/null || true

echo "  - Загрузка dds.click (device + geo)..."
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --multiquery < "${DDS_TRANSFORM_SQL}"

echo "  ✓ DDS обновлён"

# Показываем статистику DDS
echo ""
echo "Статистика DDS:"
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="SELECT 'dds.click' AS table, count() AS rows FROM dds.click UNION ALL SELECT 'dds.event', count() FROM dds.event FORMAT PrettyCompact"

# -----------------------------------------------------------------------------
# Шаг 3: DDS → DM (сводка по качеству)
# -----------------------------------------------------------------------------
echo ""
echo "Шаг 3: Обновление DM слоя (DQ summary)..."
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --multiquery < "${DM_TRANSFORM_SQL}"

echo "  ✓ DM обновлён"

# Показываем сводку по качеству
echo ""
echo "Сводка по качеству данных:"
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name FORMAT PrettyCompact"

# -----------------------------------------------------------------------------
# Итог
# -----------------------------------------------------------------------------
echo ""
echo "========================================"
echo "Batch-трансформация завершена!"
echo "========================================"
echo ""
echo "Доступные витрины для анализа:"
echo "  - dm.v_events_enriched    : Полное обогащение событий"
echo "  - dm.v_daily_traffic      : Агрегация по дням"
echo "  - dm.v_top_pages_daily    : Топ страниц"
echo "  - dm.v_dq_errors_daily    : Ошибки качества"
echo "  - dm.v_session_overview   : Обзор сессий"
echo "  - dm.v_utm_effectiveness  : Эффективность UTM"
echo "  - dm.dq_summary           : Статистика по слоям"
echo ""
echo "Подключитесь к Superset: http://localhost:8088"
