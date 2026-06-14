# Operations Runbook

Операционный runbook для локального запуска и проверки пайплайна.

## Локальный запуск

Базовые команды:

- `make up` (или `docker compose up -d`)
- `make down` (остановить и удалить контейнеры/сети проекта)
- `make clean` (полная очистка: `down -v --remove-orphans`)
- `make generated-history-analytics` (штатный чистый прогон: стартовая история
  генератора -> Kafka/STG -> ODS -> DDS -> DM -> Superset)
- `make generated-history-check` (повторяемая проверка ClickHouse и Superset
  после прогона стартовой истории)
- `make ddl` (применяет SQL из `sql/ddl/00_databases.sql` и `sql/ddl/*/*.sql` в ClickHouse)
- `make data` (архивный путь: заливает `data/*.jsonl` в Kafka; не основной источник аналитики)
- `make transform` (запускает batch-процесс ODS -> DDS -> DM)
- `make superset-init` (повторная инициализация Superset: подключение к ClickHouse, датасеты, дашборд)
- `docker compose ps`
- `docker compose logs -f --tail=200 <service>`
- `docker compose down` (сохраняет named volumes, включая `clickhouse-data`)
- `docker compose down -v` (удаляет named volumes, использовать осознанно)

## Порты

Порты задаются в `docker-compose.yml`:

- ClickHouse native: `localhost:8002` (пользователь `default`, пароль `123456`)
- ClickHouse HTTP / play-консоль: `http://localhost:9123/play` (`default` / `123456`)
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8082`
- Airflow: `http://localhost:8080` (`admin/admin`)
- Superset: `http://localhost:8088` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin/admin`)

## Airflow DAGs

Штатный аналитический путь больше не начинается с `kafka_load`: чистый стенд
получает данные из стартовой истории генератора. DAG-и ниже остаются для
ручных экспериментов, отладки и совместимости учебного стенда.

### `ddl_init`

- Запуск: ручной (`Trigger DAG`)
- Параметр: `verify_only` (`bool`, default `false`)
- Назначение: создаёт БД и таблицы в ClickHouse от `00_databases` до `40_dm`

### `kafka_load`

- Архивный путь, не основной источник аналитики.
- Запуск: ручной (`Trigger DAG with config`)
- Параметры:
  - `limit` (`int`, default `0`) — количество строк (`0` = все)
  - `reset_topics` (`bool`, default `true`) — пересоздать топики
- Примеры:

```json
{}
```

```json
{"limit": 100}
```

### `etl_pipeline`

- Запуск: ручной (`Trigger DAG with config`)
- Параметры:
  - `full_refresh` (`bool`, default `true`) — очистить DDS перед загрузкой
  - `wait_stg_timeout_sec` (`int`, default `600`, minimum `30`) — сколько секунд задача `wait_for_stg_data` ждёт появления данных в STG, прежде чем упасть по таймауту
- Зависимость: требует наличия данных в STG (от `kafka_load` или `make data`)
- В штатном сценарии STG наполняет `make generated-history-analytics` через
  backfill генератора.
- Гейт целостности DDS: `check_dds_integrity` считает события без клика, а
  `assert_dds_integrity` роняет DAG при `orphan_events > 0`. Проверка идёт после
  `load_dds` и до `load_dm_summary`, чтобы DM не собирался поверх нарушенной связи
  `dds.event -> dds.click`. Для `assert_dds_integrity` задано `retries=0`: повтор не
  чинит уже собранную сироту и только задерживает явный failed-статус.

## Генератор событий (автономный стриминг)

Автономный сервис для непрерывной генерации событий в Kafka. Работает независимо от Airflow DAGs.

### Управление

```bash
# Запустить генератор
make generator-up

# Остановить генератор
make generator-down

# Перезапуск с пересборкой
make generator-restart

# Логи
make generator-logs
```

### Конфигурация (env)

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `GEN_TICK_SECONDS` | Интервал между тиками | `5` |
| `GEN_LAMBDA_BASE_PER_MIN` | Базовая интенсивность (событий/мин) | `30` |
| `GEN_JITTER_PCT` | Процент вариативности | `20` |
| `GEN_MIN_EVENTS_PER_TICK` | Минимальный событийный бюджет тика | `1` |
| `GEN_MAX_EVENTS_PER_TICK` | Максимальный событийный бюджет тика | `50` |
| `GEN_MAX_SESSION_EVENTS` | Потолок длины одного визита | `30` |
| `GEN_MAX_ACTIVE_SESSIONS` | Потолок одновременных активных визитов | `200` |
| `GEN_POPULATION_MAX` | Потолок активной популяции пользователей | `300` |
| `GEN_P_NEW_USER` | Доля визитов новых пользователей | `0.15` |
| `GEN_MIN_RETURN_MINUTES` | Минимальная пауза перед возвратом пользователя | `30` |
| `GEN_MODEL_T0` | Стартовая модельная точка, ISO 8601 с часовым поясом | `2026-01-01T00:00:00+00:00` |
| `GEN_MODEL_T_END` | Правая граница стартовой истории для `backfill` | пусто |
| `GEN_MODEL_TIMEZONE` | Часовой пояс модельных часов для дневного коэффициента | `UTC` |
| `GEN_MODEL_TIME_SPEED` | Сколько модельных секунд проходит за одну настенную секунду | `1` |
| `GEN_RUN_MODE` | Режим генератора | `live` |
| `GEN_STATE_ENABLED` | Сохранять state v2 между рестартами | `true` |
| `GEN_STATE_RESET` | Сбросить state при старте | `false` |

В `docker-compose.yml` через окружение переопределяются демо-параметры модели и
режима, например:

```bash
GEN_STATE_RESET=true GEN_LAMBDA_BASE_PER_MIN=60 docker compose up -d generator
```

Контейнерные `KAFKA_BOOTSTRAP_SERVERS` и `GEN_DATA_DIR` в compose оставлены
внутренними значениями `kafka:29092` и `/data`.

### Стартовая история через backfill

Штатная команда чистого прогона:

```bash
make generated-history-analytics
```

Она выполняет полный сброс volumes, поднимает ClickHouse и Kafka, применяет DDL,
запускает `GEN_RUN_MODE=backfill`, прогоняет batch STG -> ODS -> DDS -> DM,
инициализирует Superset и запускает техническую проверку. Для координатора или CI
короткая повторная проверка после уже готового стенда:

```bash
make generated-history-check
```

По умолчанию команда использует быстрый проверочный профиль: 6 часов модельного
времени (`GEN_MODEL_T_END=2026-01-01T06:00:00+00:00`). Суточную историю можно
прогнать отдельно, явно задав правую границу:

```bash
GEN_MODEL_T_END=2026-01-02T00:00:00+00:00 make generated-history-analytics
```

`GEN_RUN_MODE=backfill` быстро проматывает модельное прошлое от `GEN_MODEL_T0`
до `GEN_MODEL_T_END` без сна. В Kafka попадают события только за полуоткрытый
отрезок `[T0, T_end)`. В compact-topic `generator_state` сохраняется state на
`T_end`, а в `generator_startup_history_manifest` — manifest с настройками и
контрольными числами. При live-запуске с теми же настройками генератор видит,
что state совпадает с manifest, и стартует ровно с `T_end` без настенной дельты.

Для чистого повтора пересоздавайте volumes. Это сбрасывает ClickHouse,
Kafka-топики данных, state и manifest генератора. `make generated-history-analytics`
делает это по умолчанию (`CLEAN_START=1`).

```bash
make clean
docker compose up -d clickhouse kafka
make ddl

GEN_RUN_MODE=backfill \
GEN_STATE_RESET=true \
GEN_SEED=4242 \
GEN_MODEL_T0=2026-01-01T00:00:00+00:00 \
GEN_MODEL_T_END=2026-01-01T06:00:00+00:00 \
GEN_MODEL_TIMEZONE=UTC \
GEN_MODEL_TIME_SPEED=1 \
GEN_TICK_SECONDS=60 \
GEN_LAMBDA_BASE_PER_MIN=60 \
GEN_JITTER_PCT=0 \
docker compose run --rm --no-deps generator

# Materialized View слоя STG читает Kafka сама; даём ей коротко догнать.
sleep 10
bash scripts/run_batch.sh
```

Ручной сценарий выше нужен для отладки. В обычной проверке используйте
`make generated-history-analytics`, чтобы не забыть Superset и итоговый check.

Manifest можно посмотреть так:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic generator_startup_history_manifest \
  --from-beginning \
  --property print.key=true \
  --timeout-ms 5000
```

Live-продолжение стартует с этого state. Используйте те же `GEN_SEED`, `T0`,
`T_end`, часовой пояс и настройки генерации. `GEN_STATE_RESET=false` важен: иначе
слепок стартовой истории будет проигнорирован.

```bash
docker compose run -d --name startup-history-live --no-deps \
  -e GEN_RUN_MODE=live \
  -e GEN_STATE_RESET=false \
  -e GEN_SEED=4242 \
  -e GEN_MODEL_T0=2026-01-01T00:00:00+00:00 \
  -e GEN_MODEL_T_END=2026-01-01T06:00:00+00:00 \
  -e GEN_MODEL_TIMEZONE=UTC \
  -e GEN_MODEL_TIME_SPEED=1 \
  -e GEN_TICK_SECONDS=60 \
  -e GEN_LAMBDA_BASE_PER_MIN=60 \
  -e GEN_JITTER_PCT=0 \
  generator

sleep 130
docker stop startup-history-live
docker rm startup-history-live
sleep 10
bash scripts/run_batch.sh
```

Базовая сверка формы данных после backfill:

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end
SELECT
    uniqExact(user_domain_id) AS users,
    uniqExact(click_id) AS visits,
    count() AS events,
    users < visits AND visits < events AS pyramid_ok,
    min(event_ts) AS min_event_ts,
    max(event_ts) AS max_event_ts,
    min_event_ts >= t0 AND max_event_ts < t_end AS half_open_ok
FROM dm.v_events_enriched
WHERE event_ts >= t0 AND event_ts < t_end;
```

Повторяемость чистого прогона удобно сверять коротким digest по ключевым полям
ClickHouse. Запускайте запрос после `bash scripts/run_batch.sh`; при одинаковых
`GEN_SEED`, `T0`, `T_end` и настройках значение должно повторяться.

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user=default \
  --password=123456 \
  --query "
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end
SELECT hex(sipHash128(groupArray(tuple(
    event_id,
    click_id,
    user_domain_id,
    event_ts,
    page_url_path
)))) AS digest
FROM (
    SELECT
        event_id,
        click_id,
        user_domain_id,
        event_ts,
        page_url_path
    FROM dm.v_events_enriched
    WHERE event_ts >= t0 AND event_ts < t_end
    ORDER BY event_id
  )"
```

Возвраты пользователей:

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    users AS (
        SELECT user_domain_id, uniqExact(click_id) AS visits
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
          AND user_domain_id IS NOT NULL
        GROUP BY user_domain_id
    )
SELECT
    count() AS users,
    countIf(visits > 1) AS returning_users,
    returning_users / users AS returning_share
FROM users;
```

Форма длины визита: проверяем не только среднее, а долю коротких визитов,
медиану и долю визитов, срезанных потолком `GEN_MAX_SESSION_EVENTS`.

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    30 AS max_session_events,
    sessions AS (
        SELECT
            click_id,
            count() AS events_count,
            dateDiff('second', min(event_ts), max(event_ts)) AS duration_sec
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    count() AS visits,
    countIf(events_count <= 2) / visits AS short_visit_share,
    quantileExact(0.5)(events_count) AS median_events_per_visit,
    avg(events_count) AS avg_events_per_visit,
    countIf(events_count = max_session_events) / visits AS capped_visit_share,
    quantileExact(0.5)(duration_sec) AS median_duration_sec
FROM sessions;
```

Воронка должна монотонно убывать, а доля дошедших до `/confirmation` должна быть
в согласованном коридоре для текущих настроек генератора. Для review gate
`confirmation_share` сравнивается по калибровочной форме «страница была в
визите». Строгий SQL ниже проверяет отдельное свойство: упорядоченный путь
`/home -> товары -> /cart -> /payment -> /confirmation`.

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    sessions AS (
        SELECT
            click_id,
            minIf(event_ts, page_url_path = '/home') AS home_ts,
            minIf(event_ts, page_url_path IN ('/product_a', '/product_b')) AS product_ts,
            minIf(event_ts, page_url_path = '/cart') AS cart_ts,
            minIf(event_ts, page_url_path = '/payment') AS payment_ts,
            minIf(event_ts, page_url_path = '/confirmation') AS confirmation_ts
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    countIf(home_ts IS NOT NULL) AS home,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts) AS products,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts) AS cart,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts AND payment_ts > cart_ts) AS payment,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts AND payment_ts > cart_ts AND confirmation_ts > payment_ts) AS confirmation,
    products <= home AND cart <= products AND payment <= cart AND confirmation <= payment AS monotonic_ok,
    confirmation / home AS confirmation_share
FROM sessions;
```

Калибровочная форма воронки проверяет, что страница была в визите, без строгого
порядка событий. Именно эту форму используем для сравнения `confirmation_share`
в review gate.

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    sessions AS (
        SELECT
            click_id,
            countIf(page_url_path = '/home') > 0 AS has_home,
            countIf(page_url_path IN ('/product_a', '/product_b')) > 0 AS has_product,
            countIf(page_url_path = '/cart') > 0 AS has_cart,
            countIf(page_url_path = '/payment') > 0 AS has_payment,
            countIf(page_url_path = '/confirmation') > 0 AS has_confirmation
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    countIf(has_home) AS home,
    countIf(has_home AND has_product) AS products,
    countIf(has_home AND has_product AND has_cart) AS cart,
    countIf(has_home AND has_product AND has_cart AND has_payment) AS payment,
    countIf(has_home AND has_product AND has_cart AND has_payment AND has_confirmation) AS confirmation,
    products <= home AND cart <= products AND payment <= cart AND confirmation <= payment AS monotonic_ok,
    confirmation / home AS confirmation_share
FROM sessions;
```

Стык backfill + live проверяется после короткого live-продолжения:

```sql
WITH
    toDateTime64('2026-01-01 00:00:00', 6) AS t0,
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    toDateTime64('2026-01-01 06:10:00', 6) AS t_live_end
SELECT
    count() AS events,
    uniqExact(event_id) AS unique_events,
    events - unique_events AS duplicate_events,
    countIf(event_ts = t_end) AS boundary_events,
    min(event_ts) AS min_event_ts,
    max(event_ts) AS max_event_ts
FROM dm.v_events_enriched
WHERE event_ts >= t0 AND event_ts < t_live_end;
```

Однородность визитов, переходящих через `T_end`:

```sql
WITH
    toDateTime64('2026-01-01 06:00:00', 6) AS t_end,
    crossing AS (
        SELECT
            click_id,
            min(event_ts) AS first_ts,
            max(event_ts) AS last_ts,
            groupUniqArray(user_domain_id) AS users,
            groupUniqArray(device_type) AS devices,
            groupUniqArray(os_name) AS os_names,
            groupUniqArray(geo_country) AS countries
        FROM dm.v_events_enriched
        WHERE event_ts >= t_end - INTERVAL 30 MINUTE
          AND event_ts < t_end + INTERVAL 30 MINUTE
        GROUP BY click_id
        HAVING first_ts < t_end AND last_ts >= t_end
    )
SELECT
    count() AS crossing_visits,
    countIf(
        length(users) = 1
        AND length(devices) = 1
        AND length(os_names) = 1
        AND length(countries) = 1
    ) AS homogeneous_visits,
    crossing_visits = homogeneous_visits AS context_ok
FROM crossing;
```

### Проверка модельного времени в ClickHouse

Для повторяемой проверки используйте чистый стенд и явный сброс состояния
генератора. `event_timestamp` в событиях — модельное время от `GEN_MODEL_T0`, а
не настенное время запуска процесса. При `GEN_MODEL_TIME_SPEED=K` один тик
покрывает `GEN_TICK_SECONDS * K` модельных секунд, и событийный бюджет считается
по этой модельной длительности.

```bash
make clean
docker compose up -d clickhouse kafka
make ddl

GEN_SEED=4242 \
GEN_MODEL_T0=2026-01-01T10:00:00+00:00 \
GEN_MODEL_TIMEZONE=UTC \
GEN_STATE_RESET=true \
GEN_TICK_SECONDS=60 \
GEN_LAMBDA_BASE_PER_MIN=60 \
GEN_JITTER_PCT=0 \
docker compose up -d --build generator

# Убедитесь по логам, что Tick 1 завершился, а Tick 2 ещё не стартовал.
sleep 9
docker compose logs --tail=40 generator
docker compose stop generator
sleep 5
bash scripts/run_batch.sh

docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "
SELECT
    count() AS events,
    min(event_ts) AS min_event_ts,
    max(event_ts) AS max_event_ts,
    uniqExact(event_id) AS unique_events,
    uniqExact(click_id) AS unique_clicks
FROM ods.browser_event
FORMAT Vertical"
```

Повторите блок с теми же значениями. При чистом стенде и `GEN_STATE_RESET=true`
контрольные числа должны совпасть, а `min_event_ts` должен начинаться от
`2026-01-01 10:00:00`.

### Топик истории

Генератор пишет историю батчей в топик `generator_batch_history` (JSON, ключ `batch_id`).

**Важно:** генератор требует работающей Kafka. Без Kafka генератор упадёт при старте или потеряет события.

```bash
# Чтение истории из Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic generator_batch_history \
  --from-beginning
```

### Метрики

Prometheus метрики доступны на `http://localhost:9109/metrics`:
- `generator_events_total` — счётчик отправленных событий
- `generator_publish_errors_total` — ошибки публикации
- `generator_tick_duration_seconds` — длительность тика
- `generator_last_success_timestamp` — время последнего успешного или частично
  успешного тика

### Мониторинг через Grafana

**Dashboard URL:** `http://localhost:3000/d/generator-overview`

Дашборд "Generator Overview" автоматически загружается при старте Grafana и содержит:

| Раздел | Панели | Описание |
|--------|--------|----------|
| **Overview** | Events/min | Скорость генерации событий в минуту |
| | Tick Duration | Медиана и p99 длительности тика |
| | Last Successful Tick | Время последнего успешного тика |
| **Events by Topic** | Events per Hour (24h bar chart) | Распределение событий по часам и топикам |
| | Events Rate by Topic | График по 4 топикам (browser, location, device, geo) |
| | Total Events by Topic | Суммарные счётчики по каждому топику |
| **Errors** | Total Errors | Общее число ошибок публикации |
| | Error Rate | Скорость ошибок (err/min) |
| | Errors by Topic | Ошибки разбиты по топикам |
| **Tick Statistics** | Tick Duration Distribution | p50, p95, p99 длительности тиков |
| | Events per Tick | Среднее число событий на тик |
| | Hour Factor | Текущий временной множитель (0.7/1.0/1.2) |
| **Status** | Generator Status | Статус работы (enabled/disabled) |
| | Generator Health | Статус активности (heartbeat last tick), не проверяет state save |
| | Time Since Last Tick | Время с последнего тика |
| **Info** | Полезные команды и параметры конфигурации |

#### Troubleshooting генератора

**Нет данных на дашборде:**
1. Проверить, что генератор запущен: `docker compose ps generator`
2. Проверить метрики напрямую: `curl http://localhost:9109/metrics`
3. Проверить target в Prometheus: `http://localhost:9090/targets` (job: generator)

**Высокий error rate:**
- Проверить доступность Kafka: `docker compose ps kafka`
- Смотреть логи: `make generator-logs`
- Проверить consumer lag: дашборд Kafka Overview

**Длительные тики (p99 > 1s):**
- Проверить CPU/ресурсы контейнера
- Возможно, высокая нагрузка на Kafka — проверить дашборд Kafka

**Dashboard не загрузился:**
```bash
# Перезагрузить provisioning Grafana
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload

# Или пересоздать контейнер
docker compose restart grafana
```

## Рекомендуемый сценарий

```bash
# Полный чистый путь: генерация -> STG -> ODS -> DDS -> DM -> Superset
make generated-history-analytics

# Повторная техническая проверка без пересоздания данных
make generated-history-check
```

## Быстрые проверки

- Kafka ingest: наличие данных генератора в `stg.*` и типизированных строк в `ods.*`.
- Airflow UI: `http://localhost:8080` показывает DAG `ddl_init`, `kafka_load`, `etl_pipeline`.
- BI: витрина `dm.v_events_enriched` отвечает за разумное время при фильтре по дате.

---

## Мониторинг

### TL;DR после `git pull`

```bash
# Быстрый вариант (make)
make reload-monitoring
# Если мониторинг "залип" (No data/out of bounds) — жесткое восстановление:
make recover-monitoring

# Или вручную:
docker compose up -d prometheus grafana kafka-exporter statsd-exporter
docker compose restart prometheus statsd-exporter
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload
```

Если менялся `configs/prometheus_ch.xml`: `docker compose restart clickhouse`.

### Prometheus + Grafana для ClickHouse, Kafka и Airflow

Стек мониторинга поднимается вместе с остальной инфраструктурой:

```bash
# Проверить статус сервисов мониторинга
docker compose ps prometheus grafana

# Проверить скрейп ClickHouse в Prometheus
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

### Конфигурация

- **ClickHouse**: встроенный Prometheus endpoint (`/metrics` на порту `9126`)
- **Kafka**: через `kafka-exporter` (порт `9308`)
- **Airflow**: через `statsd-exporter` (StatsD → Prometheus, порт `9102`)
  - Airflow отправляет метрики в StatsD-формате на `statsd-exporter:8125`
  - Mapping конфигурация: `configs/statsd_mapping.yml`
- **Grafana provisioning** (`configs/grafana/provisioning/`):
  - Дашборды: ClickHouse Overview, Kafka Overview, Airflow Overview, Generator Overview
  - Алерты: ClickHouse, Kafka, Airflow

### После `git pull`: быстрый апдейт мониторинга

Если прилетели изменения в `configs/grafana/provisioning/*` или `configs/prometheus.yml`, примените их так:

```bash
# Рекомендуемый способ (через make)
make reload-monitoring
# Если метрики пропали/залипли:
make recover-monitoring

# Или вручную:
docker compose rm -sf prometheus statsd-exporter
docker compose up -d prometheus grafana kafka-exporter statsd-exporter
docker compose restart airflow-scheduler airflow-webserver
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload
```

Проверка результата:

```bash
# Дашборды и алерты
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | grep -o '"title":"[^"]*"'

# Kafka метрики
curl -s http://localhost:9090/api/v1/targets | grep kafka
curl -s http://localhost:9308/metrics | grep "^kafka_brokers"
```

Если в пулле изменился `configs/prometheus_ch.xml`, дополнительно перезапустите ClickHouse:

```bash
docker compose restart clickhouse
```

Если дашборд Kafka не загрузился (ошибка "Dashboard title cannot be empty" в логах), пересоздайте контейнер Grafana:

```bash
docker compose stop grafana && docker compose rm -f grafana && docker compose up -d grafana
```

### Дашборд ClickHouse Overview

URL: `http://localhost:3000/d/clickhouse-overview/clickhouse-overview`

| Раздел | Метрики |
|--------|---------|
| System Health | CPU Usage, Memory Resident, Memory Code |
| Query Performance | Queries per Second, Active Queries, Failed Queries (total), Total Queries, Inserted Rows/sec |
| MergeTree Storage | Total Parts, Parts by State, Total Merges, Merges per Second |

Принятое решение по метрикам: сверили naming через Context7 (`/clickhouse/clickhouse-docs`, раздел Prometheus interface) и заменили недоступные в `25.1` серии на фактически экспортируемые (`ClickHouseProfileEvents_InsertedRows`, `ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables`, `ClickHouseMetrics_Parts*`).

### Дашборд Kafka Overview

URL: `http://localhost:3000/d/kafka-overview/kafka-overview`

| Раздел | Метрики |
|--------|---------|
| Cluster Health | Brokers Up, Topics, Total Partitions, Consumer Groups |
| Throughput | Messages In / sec by Topic |
| Consumers | Consumer Lag by Group |
| Partitions | Partition Offsets (Current) |

**Источник метрик:** `kafka-exporter` (danielqsj/kafka-exporter), формат конфигурации подтверждён через Context7 (`/danielqsj/kafka_exporter`, `/prometheus/docs`).

### Проверка метрик

```bash
# Prometheus собирает метрики ClickHouse
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseAsyncMetrics_MemoryResident"
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseProfileEvents_Query"

# Prometheus собирает метрики Kafka
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers"
curl -s "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag"

# Прямая проверка kafka-exporter
curl -s http://localhost:9308/metrics | grep "^kafka_"
```

```bash
# Проверить, что Prometheus собирает метрики
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseAsyncMetrics_MemoryResident"

# Проверить счётчик запросов
curl -s "http://localhost:9090/api/v1/query?query=ClickHouseProfileEvents_Query"
```

### Алерты Grafana

**ClickHouse Alerts** — provisioning-файл: `configs/grafana/provisioning/alerting/clickhouse-alert-rules.yml`

Настроены правила:
- `ClickHouse Failed Queries Rate` — `rate(ClickHouseProfileEvents_FailedQuery[5m]) > 0` в течение `2m`
- `ClickHouse Memory Resident High` — `MemoryResident / OSMemoryTotal * 100 > 85` в течение `5m`
- `ClickHouse Parts Active High` — `ClickHouseMetrics_PartsActive > 500` в течение `10m`

**Kafka Alerts** — provisioning-файл: `configs/grafana/provisioning/alerting/kafka-alert-rules.yml`

Настроены правила:
- `Kafka Broker Down` — `kafka_brokers < 1` в течение `1m`
- `Kafka Consumer Lag High` — `kafka_consumergroup_lag > 10000` в течение `5m`
- `Kafka No Messages Produced` — `rate(kafka_topic_partition_current_offset[5m]) < 0.1` в течение `10m`
- `Kafka Consumer Group Missing` не включён: для демо-стенда даёт шум на стартовых прогонах и не повышает диагностику по сравнению с lag/throughput.

Проверка и reload без рестарта контейнера:

```bash
# Список правил unified alerting
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules

# Принудительно перечитать provisioning alerting
curl -s -X POST -u admin:admin http://localhost:3000/api/admin/provisioning/alerting/reload
```

### Troubleshooting мониторинга

**Общие проблемы:**
- **"No data" в Grafana**: проверить, что Prometheus видит target (`Status -> Targets` в UI)
- **Dashboard не загрузился**: проверить логи Grafana — provisioning работает при первом старте контейнера
- **Prometheus spam `out of bounds` и дашборды пустые**: выполнить `make recover-monitoring`

**ClickHouse:**
- **Метрики не обновляются**: ClickHouse экспортирует метрики на `0.0.0.0:9126` внутри сети Docker

**Kafka:**
- **`connection refused` к Kafka**: проверить, что kafka-exporter использует `kafka:29092` (внутренняя сеть), не `localhost:9092`
- **Метрики Kafka не появляются**: проверить, что kafka-exporter подключился к Kafka — `docker compose logs kafka-exporter`
- **Нет консьюмер-групп**: kafka-exporter показывает lag только при наличии активных консьюмеров с закоммиченными offset

## Troubleshooting

- `etl_pipeline` падает с ошибкой схемы: сначала запустить `ddl_init`.
- `git pull` падает с `Permission denied` на `data/*` или `configs/grafana/provisioning/*`:
  - Причина: локально есть файлы/каталоги не вашего пользователя (часто после запуска контейнеров с root-пользователем).
  - Диагностика:
    ```bash
    ls -ld data configs/grafana/provisioning
    ls -l data | head -n 20
    ```
  - Быстрое восстановление:
    ```bash
    # Владелец и права для рабочей копии репозитория
    sudo chown -R "$USER:$USER" .
    find . -type d -exec chmod u+rwx {} \;
    find . -type f -exec chmod u+rw {} \;
    ```
  - После восстановления повторить `git pull --ff-only`.
  - Не запускать `git` через `sudo`.
- `grafana` перезапускается с ошибкой `attempt to write a readonly database`:
  - Причина: старый `grafana_lib` содержит `grafana.db`, созданный root-пользователем.
  - Простой recovery (сбросить только volume Grafana):
    ```bash
    docker compose stop grafana
    docker volume ls | grep grafana_lib
    docker volume rm <project>_grafana_lib
    docker compose up -d grafana
    ```
- В Superset ошибки `DB engine Error` и `Cannot load filter`, а в логах есть `Can't load plugin: sqlalchemy.dialects:clickhouse.connect`:
  - Причина: некорректный URI диалекта ClickHouse (`clickhouse+connect://...`).
  - Используйте URI `clickhousedb://...` и пересоберите сервисы Superset:
    ```bash
    docker compose build superset superset-init
    docker compose up -d clickhouse
    docker compose up -d --force-recreate superset-init superset
    ```
- После `docker compose down -v` нужно повторно прогнать `make generated-history-analytics`.
- После `make clean`/`down -v` Superset стартует, но витрины `dm.*` ещё пустые или
  отсутствуют до прогона стартовой истории; используйте `make generated-history-analytics`.
- Архивную загрузку `make data` использовать только для ручных экспериментов.
