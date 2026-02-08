#!/usr/bin/env bash
set -euo pipefail

# Скрипт загрузки демо‑данных в Kafka из файлов `data/*_events.jsonl`.
#
# Идея максимально простая и “как в проде”:
# - 1 строка в `.jsonl` = 1 Kafka message (value = строка JSON целиком).
# - Мы НЕ парсим и НЕ валидируем JSON, чтобы спокойно заливать “грязные” строки.
# - Kafka CLI запускаем внутри docker‑контейнера `kafka`, а сами файлы читаем на хосте.
#
# Как запускать:
#   make data                  # залить файлы целиком (по умолчанию)
#   LIMIT=100 make data        # взять первые 100 строк каждого файла
#   LIMIT=50 make data         # взять первые 50 строк каждого файла (быстрый тест)
#   RESET_TOPICS=0 make data   # не пересоздавать топики, а дописать сообщения
#
# Требования:
# - сервис `kafka` должен быть запущен (`docker compose up -d kafka` / `make up`)
# - внутри контейнера должны быть утилиты `kafka-topics.sh` и `kafka-console-producer.sh`

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

# Поведение по умолчанию: пересоздать топики и залить все данные.
RESET_TOPICS="${RESET_TOPICS:-1}"
LIMIT="${LIMIT:-}"

# Валидация параметров: лучше упасть с понятной ошибкой, чем молча сделать "не то".
if [[ "${RESET_TOPICS}" != "0" && "${RESET_TOPICS}" != "1" ]]; then
  echo "RESET_TOPICS must be 0 or 1 (got: ${RESET_TOPICS})" >&2
  exit 1
fi

if [[ -n "${LIMIT}" && "${LIMIT}" != "0" ]]; then
  if ! [[ "${LIMIT}" =~ ^[0-9]+$ ]]; then
    echo "LIMIT must be a non-negative integer (got: ${LIMIT})" >&2
    exit 1
  fi
fi

# Жёсткий маппинг “файл → топик”.
# Так проще читать и дебажить: названия топиков совпадают с тем, что ожидает DDL ClickHouse.
topic_for_file() {
  case "$1" in
    data/browser_events.jsonl) echo "browser_events" ;;
    data/location_events.jsonl) echo "location_events" ;;
    data/device_events.jsonl) echo "device_events" ;;
    data/geo_events.jsonl) echo "geo_events" ;;
    *) return 1 ;;
  esac
}

# Находим Kafka CLI внутри контейнера.
# В идеале `command -v` должен вернуть путь, но для надёжности добавлен fallback на /opt/kafka/bin.
kafka_bin() {
  local candidate="$1"
  local path
  path="$($COMPOSE_BIN exec -T "$KAFKA_SERVICE" bash -lc "command -v '$candidate' 2>/dev/null || true" | tr -d '\r')"
  if [[ -n "$path" ]]; then
    echo "$path"
    return 0
  fi

  for p in /opt/kafka/bin "/usr/bin" "/bin" "/usr/local/bin"; do
    path="$($COMPOSE_BIN exec -T "$KAFKA_SERVICE" bash -lc "test -x '$p/$candidate' && echo '$p/$candidate' || true" | tr -d '\r')"
    if [[ -n "$path" ]]; then
      echo "$path"
      return 0
    fi
  done

  return 1
}

KAFKA_TOPICS_BIN="$(kafka_bin kafka-topics.sh)"
KAFKA_PRODUCER_BIN="$(kafka_bin kafka-console-producer.sh)"

if [[ -z "$KAFKA_TOPICS_BIN" ]]; then
  echo "kafka-topics.sh not found inside service '$KAFKA_SERVICE'." >&2
  exit 1
fi

if [[ -z "$KAFKA_PRODUCER_BIN" ]]; then
  echo "kafka-console-producer.sh not found inside service '$KAFKA_SERVICE'." >&2
  exit 1
fi

topics=(browser_events location_events device_events geo_events)

# “Reset” топиков — самый простой способ делать повторяемые прогоны:
# удалили топики → создали заново → offsets тоже начинаются “с нуля”.
if [[ "$RESET_TOPICS" == "1" ]]; then
  echo "Resetting topics: ${topics[*]}"
  for t in "${topics[@]}"; do
    $COMPOSE_BIN exec -T "$KAFKA_SERVICE" "$KAFKA_TOPICS_BIN" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --delete --if-exists \
      --topic "$t" >/dev/null || true
  done

  for t in "${topics[@]}"; do
    $COMPOSE_BIN exec -T "$KAFKA_SERVICE" "$KAFKA_TOPICS_BIN" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --create --if-not-exists \
      --topic "$t" \
      --partitions 1 \
      --replication-factor 1 >/dev/null
  done
else
  echo "RESET_TOPICS=0: topics will not be reset."
fi

# Ищем входные файлы. `nullglob` нужен, чтобы шаблон без матчей не превратился в строку.
shopt -s nullglob
files=(data/*_events.jsonl)
shopt -u nullglob

if [[ "${#files[@]}" -eq 0 ]]; then
  echo "No input files found: data/*_events.jsonl" >&2
  exit 1
fi

# Выбираем режим загрузки:
# - full: весь файл
# - slice: первые N строк (быстрее для отладки)
mode="slice"
if [[ "$LIMIT" == "0" || -z "$LIMIT" ]]; then
  mode="full"
fi

echo "Loading mode: ${mode} (LIMIT=${LIMIT:-unset})"
echo "Bootstrap (inside container): ${BOOTSTRAP_SERVER}"

for f in "${files[@]}"; do
  if ! t="$(topic_for_file "$f")"; then
    echo "Skipping unknown file (no topic mapping): $f" >&2
    continue
  fi

  # Важный момент:
  # `kafka-console-producer.sh` читает stdin построчно и отправляет каждую строку отдельным message.
  if [[ "$mode" == "full" ]]; then
    echo "Publishing: $f -> $t (full)"
    cat "$f" | $COMPOSE_BIN exec -T "$KAFKA_SERVICE" "$KAFKA_PRODUCER_BIN" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --topic "$t" >/dev/null
  else
    echo "Publishing: $f -> $t (first $LIMIT lines)"
    head -n "$LIMIT" "$f" | $COMPOSE_BIN exec -T "$KAFKA_SERVICE" "$KAFKA_PRODUCER_BIN" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --topic "$t" >/dev/null
  fi
done

echo "Done."
