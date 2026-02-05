# Runbook: запуск демо и загрузка данных

Этот документ фиксирует порядок действий и `make`‑таргеты. Он не описывает внутренности ClickHouse‑слоёв (это в `plans/clickhouse_ddl.md`).

## Предпосылки

- Docker + Docker Compose.
- Доступ к Docker daemon (если `docker compose ...` пишет `permission denied ... /var/run/docker.sock`, добавьте пользователя в группу `docker` или запускайте команды с правами, принятыми в вашей среде).

## Быстрый сценарий

1) Поднять инфраструктуру:

```bash
make up
```

2) (Опционально) Залить данные в Kafka:

```bash
make data
```

`make data` не зависит от ClickHouse/DDL — достаточно, чтобы Kafka была поднята.

3) Применить DDL в ClickHouse:

```bash
make ddl
```

## Make таргеты

- `make up` — `docker compose up -d` (поднимает весь стек из `docker-compose.yml`).
- `make ddl` — применяет SQL из `plans/clickhouse_ddl.md` в контейнер ClickHouse (извлекает все блоки ```sql``` и исполняет их через `clickhouse-client`).
- `make data` — пересоздаёт топики (по умолчанию) и публикует события из `data/*.jsonl` в Kafka (1 строка = 1 Kafka message value).

## Загрузка данных в Kafka (`make data`)

### Топики

Скрипт использует фиксированный маппинг:

- `data/browser_events.jsonl` → `browser_events`
- `data/location_events.jsonl` → `location_events`
- `data/device_events.jsonl` → `device_events`
- `data/geo_events.jsonl` → `geo_events`

### Режимы загрузки

- По умолчанию — “debug срез”: первые 50 строк каждого файла.
- Полная загрузка — весь файл.

Параметры (env):

- `LIMIT` — сколько строк брать из каждого `.jsonl` (по умолчанию `50`). `LIMIT=0` трактуется как “весь файл”.
- `FULL` — если `FULL=1`, грузит весь файл независимо от `LIMIT`.
- `RESET_TOPICS` — если `RESET_TOPICS=1` (по умолчанию), топики удаляются и создаются заново с теми же именами.
- `BOOTSTRAP_SERVER` — bootstrap для Kafka *изнутри kafka‑контейнера* (по умолчанию `kafka:29092`).

Примеры:

```bash
# 100 строк на поток
LIMIT=100 make data

# Полная заливка всех строк
FULL=1 make data

# Дозалить данные без пересоздания топиков
RESET_TOPICS=0 make data
```

## Применение DDL в ClickHouse (`make ddl`)

Скрипт исполняет SQL из `plans/clickhouse_ddl.md`. Для `ENGINE = Kafka` важно, чтобы `kafka_broker_list` был доступен из контейнера ClickHouse.

В текущем compose:

- для соединений “контейнер → Kafka” используйте `kafka:29092`;
- `localhost:9092` подходит только для клиентов на хосте.

