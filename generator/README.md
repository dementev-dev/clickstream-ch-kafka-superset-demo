# Генератор событий (MVP)

Автономный сервис для стриминга событий в Kafka.

## Архитектура

```
generator-service -> Kafka topics -> (потребители отдельно)
```

Генератор работает автономно и не зависит от потребителей (Airflow, ClickHouse).

## Режим работы: `steady`

- Каждую минуту публикуем переменный объём событий (Poisson + jitter)
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
| `GEN_TICK_SECONDS` | Интервал между тиками | `60` |
| `GEN_LAMBDA_BASE_PER_MIN` | Базовая интенсивность (событий/мин) | `200` |
| `GEN_JITTER_PCT` | Процент вариативности | `20` |
| `GEN_MIN_EVENTS_PER_TICK` | Минимум событий за тик | `50` |
| `GEN_MAX_EVENTS_PER_TICK` | Максимум событий за тик | `500` |
| `GEN_DATA_DIR` | Путь к JSONL файлам | `/data` |
| `GEN_SEED` | Сид для воспроизводимости | — |
| `GEN_ENABLED` | Включить генерацию | `true` |

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

## Логи и метрики

### Логи

```
=== Tick 1 (batch_id=a1b2c3d4) ===
Generating ~156 base events
Batch a1b2c3d4 completed: sent=624, errors=0, gen_time=0.012s, pub_time=0.234s, total_time=0.247s
  browser_events: 156 sent
  location_events: 156 sent
  device_events: 156 sent
  geo_events: 156 sent
Sleeping for 59.8s until next tick
```

### Метрики (в коде)

- `generator_events_total` — всего отправлено событий
- `generator_publish_errors_total` — ошибки публикации
- `generator_tick_duration_seconds` — длительность тика

## Тестирование

### Локальные тесты

```bash
# Базовые тесты
docker run --rm -v $(pwd)/..:/workspace -w /workspace/generator generator:test python test_local.py

# Комплексные тесты (проверка граничных случаев, статистики, формата)
docker run --rm -v $(pwd)/..:/workspace -w /workspace/generator generator:test python test_comprehensive.py
```

### Интеграционный тест

```bash
# Запустить стек с генератором
make generator-up

# Проверить логи
make generator-logs

# Проверить сообщения в Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic browser_events --from-beginning
```

## История batch

Хранится в памяти (последние 1000 записей). Поля:

- `batch_id` — идентификатор батча
- `started_at` / `finished_at` — время начала/окончания
- `sent_total` — всего отправлено
- `sent_browser/location/device/geo` — по топикам
- `status` — success/partial/error
