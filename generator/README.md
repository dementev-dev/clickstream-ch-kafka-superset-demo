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
| `GEN_STATE_ENABLED` | Сохранять состояние между рестартами | `true` |
| `GEN_STATE_RESET` | Сбросить состояние при старте | `false` |

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

# Запуск тестов
make generator-test
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

История пишется в Kafka-топик `generator_batch_history` (JSON).

**Контракт топика:**
- Название фиксировано: `generator_batch_history` (не конфигурируется)
- Формат: JSON с ключом `batch_id`

**Важно:** генератор требует работающей Kafka. Без Kafka генератор упадёт при старте или потеряет события. Для мониторинга доступности используйте Prometheus-метрики (`generator_last_success_timestamp`).

Поля сообщения:
- `batch_id` — идентификатор батча
- `started_at` / `finished_at` — время начала/окончания (ISO format)
- `sent_total` — всего отправлено
- `sent_browser/location/device/geo` — по топикам
- `status` — success/partial/error
- `error_message` — описание ошибки (если есть)

### Чтение истории из Kafka

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic generator_batch_history \
  --from-beginning
```

## State Recovery (восстановление состояния)

Генератор сохраняет своё состояние между перезапусками в Kafka-топик `generator_state` (compact topic). Это позволяет:

- Продолжить нумерацию тиков с места остановки
- Сохранить последовательность случайных чисел (RNG state)
- Избежать дублирования при рестарте

### Как работает

1. После каждого успешного тика состояние сохраняется в `generator_state`
2. При старте генератор читает последнее состояние из топика
3. Если состояние найдено - продолжает с сохранённого tick
4. Если нет - начинает с tick=1

### Топик `generator_state`

- **Название**: фиксировано `generator_state`
- **Тип**: compact topic (хранится только последнее значение для каждого ключа)
- **Ключ**: `default` (для возможности нескольких генераторов в будущем)
- **Конфигурация**: `cleanup.policy=compact`, минимальный retention

### Просмотр текущего состояния

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic generator_state \
  --from-beginning \
  --property print.key=true
```

### Сброс состояния (начать сначала)

```bash
# Вариант 1: через env (рекомендуется)
GEN_STATE_RESET=true docker compose up -d generator

# Вариант 2: удалить топик полностью
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --delete \
  --topic generator_state
```

### Отключение сохранения состояния

```bash
GEN_STATE_ENABLED=false docker compose up -d generator
```

При отключенном state management генератор всегда начинает с tick=1, RNG инициализируется с GEN_SEED (или случайно).

## Тестирование

Тесты написаны на **pytest**.

### Запуск тестов

```bash
# Через Makefile (рекомендуется)
make generator-test

# Вручную через Docker
docker build -t generator:test .
docker run --rm -v $(PWD):/workspace -w /workspace/generator generator:test pytest tests/ -v

# Конкретный файл тестов
docker run --rm -v $(PWD):/workspace -w /workspace/generator generator:test pytest tests/test_generation.py -v
```

### Структура тестов

```
generator/tests/
├── conftest.py           # Fixtures pytest
├── test_config.py        # Тесты конфигурации
├── test_generation.py    # Тесты генерации событий
├── test_history.py       # Тесты структуры BatchRecord
├── test_kafka_history.py # Тесты KafkaBatchHistory
├── test_service.py       # Тесты GeneratorService
└── test_state.py         # Тесты GeneratorState и KafkaStateManager
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
