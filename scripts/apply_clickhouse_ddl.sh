#!/usr/bin/env bash
#
# Скрипт применения DDL в ClickHouse
#
# Назначение:
#   Последовательно применяет SQL-файлы из sql/ddl/* в базу ClickHouse.
#   Порядок фиксированный: 00 → 10 → 20 → 30 → 40.
#
# Как запускать:
#   make ddl
#   или: bash scripts/apply_clickhouse_ddl.sh
#
# Требования:
#   - Сервис clickhouse должен быть запущен (make up)
#   - Доступен clickhouse-client внутри контейнера
#
# Порядок применения важен:
#   00_databases.sql → 10_stg.sql → 20_ods.sql → 30_dds.sql → 40_dm.sql
#

set -euo pipefail

# Директория со скриптом
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_ROOT_DIR="${SCRIPT_DIR}/../sql"

# Параметры подключения (можно переопределить через переменные окружения)
COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"

echo "Применение DDL из ${SQL_ROOT_DIR}..."

# -----------------------------------------------------------------------------
# Проверка: ClickHouse запущен?
# -----------------------------------------------------------------------------
if ! ${COMPOSE_BIN} ps | grep -q "${CLICKHOUSE_SERVICE}"; then
    echo "Ошибка: Сервис '${CLICKHOUSE_SERVICE}' не запущен."
    echo "Запустите сначала: make up"
    exit 1
fi

# -----------------------------------------------------------------------------
# Применение SQL-файлов по порядку
# -----------------------------------------------------------------------------
DDL_FILES=(
    "${SQL_ROOT_DIR}/ddl/00_databases.sql"
    "${SQL_ROOT_DIR}/ddl/stg/10_stg.sql"
    "${SQL_ROOT_DIR}/ddl/ods/20_ods.sql"
    "${SQL_ROOT_DIR}/ddl/dds/30_dds.sql"
    "${SQL_ROOT_DIR}/ddl/dm/40_dm.sql"
)

for sql_file in "${DDL_FILES[@]}"; do
    if [[ ! -f "$sql_file" ]]; then
        echo "Ошибка: не найден SQL-файл: $sql_file" >&2
        exit 1
    fi

    echo "Применение: $(basename "$sql_file")"
    ${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
        --user="${CLICKHOUSE_USER}" \
        --password="${CLICKHOUSE_PASSWORD}" \
        --database="${CLICKHOUSE_DB}" \
        --multiquery \
        < "$sql_file"
    echo "  ✓ OK"
done

echo ""
echo "DDL успешно применён!"
echo ""
echo "Созданы базы данных:"
echo "  - stg : Staging (сырые данные)"
echo "  - ods : Operational Data Store (типизированные данные)"
echo "  - dds : Detailed Data Store (детальные сущности)"
echo "  - dm  : Data Marts (витрины для BI)"
