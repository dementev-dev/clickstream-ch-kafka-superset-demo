#!/usr/bin/env bash
#
# Чистый прогон аналитического контура от стартовой истории генератора до DM и Superset.
#
# Команда намеренно очищает volumes по умолчанию: так сбрасываются ClickHouse,
# Kafka-топики данных, state и manifest генератора. Это штатный повторяемый путь
# для проверки ADR-0006 на свежем стенде.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLEAN_START="${CLEAN_START:-1}"
WAIT_CLICKHOUSE_SECONDS="${WAIT_CLICKHOUSE_SECONDS:-60}"
WAIT_STG_SECONDS="${WAIT_STG_SECONDS:-10}"

GEN_SEED="${GEN_SEED:-4242}"
GEN_MODEL_T0="${GEN_MODEL_T0:-2026-01-01T00:00:00+00:00}"
GEN_MODEL_T_END="${GEN_MODEL_T_END:-2026-01-01T06:00:00+00:00}"
GEN_MODEL_TIMEZONE="${GEN_MODEL_TIMEZONE:-UTC}"
GEN_MODEL_TIME_SPEED="${GEN_MODEL_TIME_SPEED:-1}"
GEN_TICK_SECONDS="${GEN_TICK_SECONDS:-60}"
GEN_LAMBDA_BASE_PER_MIN="${GEN_LAMBDA_BASE_PER_MIN:-60}"
GEN_JITTER_PCT="${GEN_JITTER_PCT:-0}"
GEN_MIN_EVENTS_PER_TICK="${GEN_MIN_EVENTS_PER_TICK:-1}"
GEN_MAX_EVENTS_PER_TICK="${GEN_MAX_EVENTS_PER_TICK:-1000}"

cd "${REPO_ROOT}"

wait_for_clickhouse() {
  local deadline
  deadline=$((SECONDS + WAIT_CLICKHOUSE_SECONDS))

  until ${COMPOSE_BIN} exec -T clickhouse clickhouse-client \
      --user=default \
      --password=123456 \
      --query "SELECT 1" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Ошибка: ClickHouse не ответил за ${WAIT_CLICKHOUSE_SECONDS} сек." >&2
      exit 1
    fi
    sleep 2
  done
}

echo "=== Чистый прогон стартовой истории как источника аналитики ==="
echo "GEN_SEED=${GEN_SEED}"
echo "GEN_MODEL_T0=${GEN_MODEL_T0}"
echo "GEN_MODEL_T_END=${GEN_MODEL_T_END}"
echo "GEN_LAMBDA_BASE_PER_MIN=${GEN_LAMBDA_BASE_PER_MIN}"
echo ""

if [[ "${CLEAN_START}" == "1" ]]; then
  echo "Шаг 0: очистка volumes ClickHouse/Kafka/state"
  ${COMPOSE_BIN} down -v --remove-orphans
else
  echo "Шаг 0: CLEAN_START=0, очистка пропущена"
fi

echo "Шаг 1: запуск ClickHouse и Kafka"
${COMPOSE_BIN} up -d clickhouse kafka
wait_for_clickhouse

echo "Шаг 2: применение DDL"
bash "${SCRIPT_DIR}/apply_clickhouse_ddl.sh"

echo "Шаг 3: сборка образа генератора"
${COMPOSE_BIN} build generator

echo "Шаг 4: backfill стартовой истории в Kafka"
${COMPOSE_BIN} run --rm --no-deps \
  -e GEN_RUN_MODE=backfill \
  -e GEN_STATE_RESET=true \
  -e GEN_SEED="${GEN_SEED}" \
  -e GEN_MODEL_T0="${GEN_MODEL_T0}" \
  -e GEN_MODEL_T_END="${GEN_MODEL_T_END}" \
  -e GEN_MODEL_TIMEZONE="${GEN_MODEL_TIMEZONE}" \
  -e GEN_MODEL_TIME_SPEED="${GEN_MODEL_TIME_SPEED}" \
  -e GEN_TICK_SECONDS="${GEN_TICK_SECONDS}" \
  -e GEN_LAMBDA_BASE_PER_MIN="${GEN_LAMBDA_BASE_PER_MIN}" \
  -e GEN_JITTER_PCT="${GEN_JITTER_PCT}" \
  -e GEN_MIN_EVENTS_PER_TICK="${GEN_MIN_EVENTS_PER_TICK}" \
  -e GEN_MAX_EVENTS_PER_TICK="${GEN_MAX_EVENTS_PER_TICK}" \
  generator

echo "Шаг 5: ожидание чтения Kafka Materialized View (${WAIT_STG_SECONDS} сек.)"
sleep "${WAIT_STG_SECONDS}"

echo "Шаг 6: batch STG -> ODS -> DDS -> DM"
bash "${SCRIPT_DIR}/run_batch.sh"

echo "Шаг 7: инициализация Superset metadata и dashboard"
${COMPOSE_BIN} up -d postgres-metadata clickhouse
${COMPOSE_BIN} up --abort-on-container-exit --exit-code-from superset-init superset-init
${COMPOSE_BIN} up -d --no-deps superset

echo "Шаг 8: техническая проверка аналитического контура"
GEN_MODEL_T0="${GEN_MODEL_T0}" \
GEN_MODEL_T_END="${GEN_MODEL_T_END}" \
COMPOSE_BIN="${COMPOSE_BIN}" \
bash "${SCRIPT_DIR}/check_generated_analytics.sh"

echo ""
echo "Готово: стартовая история генератора доведена до DM и Superset."
