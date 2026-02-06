#!/usr/bin/env bash
set -euo pipefail

# Run batch transformations: ODS → DDS → DM
# Usage: make transform
#        or: bash scripts/run_batch.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="${SCRIPT_DIR}/../jobs"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"

# Check if clickhouse service is running
if ! ${COMPOSE_BIN} ps | grep -q "${CLICKHOUSE_SERVICE}"; then
    echo "Error: ClickHouse service '${CLICKHOUSE_SERVICE}' is not running."
    echo "Run 'make up' first to start the services."
    exit 1
fi

# Check if ODS has data
ODS_COUNT=$(${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="SELECT count() FROM ods.browser_event" 2>/dev/null || echo "0")

if [[ "${ODS_COUNT}" == "0" ]]; then
    echo "Warning: ODS.browser_event is empty."
    echo "Run 'make data' first to load data into Kafka → STG → ODS."
    exit 1
fi

echo "Found ${ODS_COUNT} rows in ODS.browser_event"
echo ""

# Step 1: Refresh DDS (truncate + reload for demo)
echo "Step 1: Refreshing DDS layer (ODS → DDS)..."
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

${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --multiquery < "${JOBS_DIR}/30_dds_refresh.sql"

echo "  ✓ DDS refreshed"

# Show DDS stats
echo ""
echo "DDS statistics:"
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="SELECT 'dds.click' AS table, count() AS rows FROM dds.click UNION ALL SELECT 'dds.event', count() FROM dds.event FORMAT PrettyCompact"

# Step 2: Refresh DM (DQ summary)
echo ""
echo "Step 2: Refreshing DM layer (DQ summary)..."
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --multiquery < "${JOBS_DIR}/40_dm_refresh.sql"

echo "  ✓ DM refreshed"

# Show DM stats
echo ""
echo "Data Quality summary:"
${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --database="${CLICKHOUSE_DB}" \
    --query="SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name FORMAT PrettyCompact"

echo ""
echo "Batch transformation complete!"
echo ""
echo "Available data marts:"
echo "  - dm.v_events_enriched    : Main enriched events view"
echo "  - dm.v_daily_traffic      : Daily aggregation by dimensions"
echo "  - dm.v_top_pages_daily    : Top pages by day"
echo "  - dm.v_dq_errors_daily    : Data quality errors"
echo "  - dm.v_session_overview   : Session-level metrics"
echo "  - dm.v_utm_effectiveness  : UTM campaign performance"
echo "  - dm.dq_summary           : Layer statistics"
