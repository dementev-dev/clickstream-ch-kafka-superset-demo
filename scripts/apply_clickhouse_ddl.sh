#!/usr/bin/env bash
set -euo pipefail

# Apply ClickHouse DDL files in order
# Usage: make ddl
#        or: bash scripts/apply_clickhouse_ddl.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DDL_DIR="${SCRIPT_DIR}/../ddl"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"

echo "Applying ClickHouse DDL from ${DDL_DIR}..."

# Check if clickhouse service is running
if ! ${COMPOSE_BIN} ps | grep -q "${CLICKHOUSE_SERVICE}"; then
    echo "Error: ClickHouse service '${CLICKHOUSE_SERVICE}' is not running."
    echo "Run 'make up' first to start the services."
    exit 1
fi

# Apply DDL files in order (00 -> 10 -> 20 -> 30 -> 40)
for sql_file in "${DDL_DIR}"/*.sql; do
    if [[ -f "$sql_file" ]]; then
        echo "Applying: $(basename "$sql_file")"
        ${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
            --user="${CLICKHOUSE_USER}" \
            --password="${CLICKHOUSE_PASSWORD}" \
            --database="${CLICKHOUSE_DB}" \
            --multiquery \
            < "$sql_file"
        echo "  ✓ OK"
    fi
done

echo "DDL applied successfully."
