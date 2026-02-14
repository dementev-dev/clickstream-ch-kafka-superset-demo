# Генератор событий (MVP rev5)

Автономный генератор событий для Kafka с режимом `steady-stream`.

## Архитектура

```
generator-service -> Kafka topics -> (потребители отдельно)
```

Генератор работает автономно и не зависит от потребителей (Airflow, ClickHouse).

## Режим работы: `steady-stream`

- Публикуем постепенно, **короткими тиками** (по умолчанию каждые 5 секунд)
- На каждом тике отправляем небольшую порцию сообщений
- Держим целевую интенсивность `events/min` без крупных минутных batch
- Распределяем события по 4 топикам:
  - `browser_events`
  - `location_events`
  - `device_events`
  - `geo_events`
- Сохраняем связи `event_id <-> location`, `click_id <-> device/geo`

## Конфигурация (env)

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес Kafka | `kafka:29092` |
| `GEN_TICK_SECONDS` | Интервал между тиками | `5` (1-10 сек рекомендуется) |
| `GEN_LAMBDA_BASE_PER_MIN` | Базовая интенсивность (событий/мин) | `200` |
| `GEN_JITTER_PCT` | Процент вариативности | `20` |
| `GEN_MIN_EVENTS_PER_TICK` | Минимум событий за тик | `5` |
| `GEN_MAX_EVENTS_PER_TICK` | Максимум событий за тик | `50` |
| `GEN_DATA_DIR` | Путь к JSONL файлам | `/data` |
| `GEN_SEED` | Сид для воспроизводимости | — |
| `GEN_ENABLED` | Включить генерацию | `true` |
| `GEN_METRICS_PORT` | Порт для Prometheus | `9109` |
| `CLICKHOUSE_HOST` | Хост ClickHouse для истории | `clickhouse` |
| `CLICKHOUSE_PORT` | Порт ClickHouse | `9000` |

### Режим "раз в минуту" (для демо)

Для контролируемых демо можно установить:
```bash
GEN_TICK_SECONDS=60
GEN_MIN_EVENTS_PER_TICK=50
GEN_MAX_EVENTS_PER_TICK=500
```

## Управление через Makefile

```bash
# Запустить только генератор
make generator-up

# Остановить генератор
make generator-down

# Смотреть логи
make generator-logs

# Перезапуск с пересборкой
make generator-restart
```

## Метрики Prometheus

Генератор экспортирует метрики на `:9109/metrics`:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `generator_events_total` | Counter | Всего отправлено событий (по топикам) |
| `generator_publish_errors_total` | Counter | Ошибки публикации (по топикам) |
| `generator_tick_duration_seconds` | Histogram | Длительность тика |
| `generator_last_success_timestamp` | Gauge | Время последнего успешного тика |

### Проверка метрик

```bash
curl http://localhost:9109/metrics
curl http://localhost:9090/api/v1/targets | grep generator
```

## История batch

История сохраняется в таблице `meta.generator_batches` (ClickHouse):

```sql
SELECT 
    batch_id,
    started_at,
    sent_total,
    status
FROM meta.generator_batches
ORDER BY started_at DESC
LIMIT 10
```

Поля:
- `batch_id` — идентификатор батча
- `started_at` / `finished_at` — время начала/окончания
- `sent_total` — всего отправлено
- `sent_browser/location/device/geo` — по топикам
- `status` — success/partial/error
- `error_message` — описание ошибки (если есть)

## Тестирование

### Локальные тесты

```bash
# Сборка образа для тестов
docker build -t generator:test .

# Базовые тесты
docker run --rm -v $(pwd)/..:/workspace -w /workspace/generator generator:test python test_local.py

# Комплексные тесты
docker run --rm -v $(pwd)/..:/workspace -w /workspace/generator generator:test python test_comprehensive.py
```

### Интеграционный тест

```bash
# Запустить стек с генератором
make generator-up

# Проверить логи
make generator-logs

# Проверить метрики
curl http://localhost:9109/metrics

# Проверить сообщения в Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic browser_events --from-beginning
```
