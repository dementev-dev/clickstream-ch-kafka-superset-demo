# План миграции ETL-оркестрации на Airflow

## Цель
Заменить `make`-команды на полноценную оркестрацию через Airflow DAGs с сохранением логики пайплайна STG → ODS → DDS → DM.

---

## Архитектура потока данных (напоминание)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Источник: data/*.jsonl → Kafka → STG (Kafka Engine) → ODS (MV)            │
│                                                                             │
│  STG → ODS: real-time через Materialized Views (не требует оркестрации)    │
│  ODS → DDS: batch через SQL (argMax + JOIN) — требует оркестрации          │
│  DDS → DM:  batch через SQL (TRUNCATE + INSERT) — требует оркестрации      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Структура DAG'ов

### 1. `ddl_init` — Инициализация схемы БД

**Назначение:** Создание всех объектов ClickHouse (базы, таблицы, MV, витрины).

**Параметры:**
- `schedule`: `None` (только ручной запуск)
- `tags`: `["ddl", "init", "clickhouse"]`

**Задачи (Tasks):**

| Task ID | Описание | Тип оператора |
|---------|----------|---------------|
| `check_clickhouse` | Проверка доступности ClickHouse | `BashOperator` — `clickhouse-client --query="SELECT 1"` |
| `create_databases` | Создание БД: stg, ods, dds, dm | `SQLExecuteQueryOperator` — `ddl/00_databases.sql` |
| `create_stg` | Таблицы STG + Kafka Engine + MV | `SQLExecuteQueryOperator` — `ddl/10_stg.sql` |
| `create_ods` | Таблицы ODS + MV STG→ODS | `SQLExecuteQueryOperator` — `ddl/20_ods.sql` |
| `create_dds` | Таблицы DDS (batch-загрузка) | `SQLExecuteQueryOperator` — `ddl/30_dds.sql` |
| `create_dm` | Витрины DM (VIEW) | `SQLExecuteQueryOperator` — `ddl/40_dm.sql` |
| `verify_schema` | Проверка: список созданных таблиц | `BashOperator` — запрос `SHOW TABLES FROM each DB` |

**Зависимости:**
```
check_clickhouse >> create_databases >> [create_stg, create_ods, create_dds, create_dm] >> verify_schema
```

**Замечание:** Порядок важен — сначала `stg`+`ods` (MV работают сразу), потом `dds`+`dm`.

---

### 2. `kafka_load` — Загрузка данных в Kafka

**Назначение:** Загрузка JSON-данных из `data/*.jsonl` в Kafka-топики.

**Параметры:**
- `schedule`: `None` (только ручной запуск)
- `tags`: `["kafka", "ingest", "demo"]`
- `params`:
  - `limit`: int — количество строк для загрузки (default: 50, 0 = все)
  - `full_load`: bool — загрузить полные файлы
  - `reset_topics`: bool — пересоздать топики (default: true)

**Задачи (Tasks):**

| Task ID | Описание | Тип оператора |
|---------|----------|---------------|
| `check_kafka` | Проверка доступности Kafka | `BashOperator` — `kafka-topics.sh --list` |
| `reset_topics` | Удаление/создание топиков (conditional) | `BashOperator` — `kafka-topics.sh --delete/--create` |
| `load_browser` | Загрузка browser_events.jsonl | `BashOperator` — `kafka-console-producer.sh` |
| `load_location` | Загрузка location_events.jsonl | `BashOperator` — `kafka-console-producer.sh` |
| `load_device` | Загрузка device_events.jsonl | `BashOperator` — `kafka-console-producer.sh` |
| `load_geo` | Загрузка geo_events.jsonl | `BashOperator` — `kafka-console-producer.sh` |
| `verify_load` | Проверка: количество сообщений в топиках | `BashOperator` — `kafka-console-consumer.sh --from-beginning` или проверка через CH |

**Зависимости:**
```
check_kafka >> reset_topics >> [load_browser, load_location, load_device, load_geo] >> verify_load
```

**Особенности:**
- Загрузка файлов может идти параллельно (независимые топики).
- `head -n {{ params.limit }}` для среза данных.

---

### 3. `etl_batch_transform` — Batch-трансформация ODS → DDS → DM

**Назначение:** Основной ETL-пайплайн — сборка сущностей и обновление витрин.

**Параметры:**
- `schedule`: `"@once"` для демо или `"*/15 * * * *"` (каждые 15 мин)
- `tags`: `["etl", "batch", "dds", "dm"]`
- `params`:
  - `full_refresh`: bool — полная перезагрузка или инкремент (default: true для демо)

**Задачи (Tasks):**

| Task ID | Описание | Тип оператора |
|---------|----------|---------------|
| `wait_for_ods` | Ожидание появления данных в ODS | `BashOperator` — `SELECT count() FROM ods.browser_event` |
| `check_ods_quality` | Проверка качества ODS: ошибки парсинга | `SQLExecuteQueryOperator` — `SELECT layer, count() FROM ods.browser_event WHERE ...` |
| `truncate_dds` | Очистка DDS таблиц (conditional) | `SQLExecuteQueryOperator` — `TRUNCATE TABLE dds.click, dds.event` |
| `refresh_dds_click` | Загрузка dds.click (device + geo) | `SQLExecuteQueryOperator` — `jobs/30_dds_refresh.sql` (часть для click) |
| `refresh_dds_event` | Загрузка dds.event (browser + location) | `SQLExecuteQueryOperator` — `jobs/30_dds_refresh.sql` (часть для event) |
| `check_dds_integrity` | Проверка: orphan events (есть event, нет click) | `SQLExecuteQueryOperator` — `SELECT count() FROM dds.event WHERE click_id NOT IN (...)` |
| `refresh_dm_summary` | Обновление dm.dq_summary | `SQLExecuteQueryOperator` — `jobs/40_dm_refresh.sql` |
| `validate_dm` | Проверка: dq_summary не пустая | `BashOperator` — `SELECT * FROM dm.dq_summary` |

**Зависимости:**
```
wait_for_ods >> check_ods_quality >> truncate_dds >> [refresh_dds_click, refresh_dds_event] >> check_dds_integrity >> refresh_dm_summary >> validate_dm
```

**Особенности:**
- `refresh_dds_click` и `refresh_dds_event` независимы — можно параллельно.
- Для инкрементальной загрузки (в будущем) понадобится watermark (src_ingest_ts).

---

### 4. `data_quality_monitor` — Мониторинг качества данных (опциональный)

**Назначение:** Регулярная проверка DQ-метрик и алерты.

**Параметры:**
- `schedule`: `"0 */1 * * *"` (каждый час)
- `tags`: `["dq", "monitoring", "alerts"]`

**Задачи (Tasks):**

| Task ID | Описание | Тип оператора |
|---------|----------|---------------|
| `check_stg_volume` | Проверка объёма STG | `SQLExecuteQueryOperator` |
| `check_ods_errors` | Проверка ошибок парсинга ODS | `SQLExecuteQueryOperator` — `SELECT count() FROM ods.*_errors` |
| `check_dds_orphans` | Проверка сиротских записей | `SQLExecuteQueryOperator` |
| `send_alert` | Отправка алерта (если проблемы) | `EmptyOperator` или callback |

**Зависимости:** Линейная цепочка с условными переходами.

---

## Технические детали реализации

### Подключение к ClickHouse

```python
# Connection в Airflow UI (Admin → Connections)
conn_id = "clickhouse_default"
conn_type = "generic"
host = "clickhouse"
port = 8123  # HTTP interface
login = "default"
password = "123456"
```

### SQL-операторы

Для выполнения SQL использовать `SQLExecuteQueryOperator` с `clickhouse-connect`:

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

refresh_dds = SQLExecuteQueryOperator(
    task_id="refresh_dds_click",
    conn_id="clickhouse_default",
    sql="""
        INSERT INTO dds.click
        SELECT ...  -- SQL из jobs/30_dds_refresh.sql
    """
)
```

### Bash-операторы для Kafka

```python
from airflow.operators.bash import BashOperator

load_browser = BashOperator(
    task_id="load_browser",
    bash_command="""
        head -n {{ params.limit }} /opt/airflow/data/browser_events.jsonl | \
        kafka-console-producer.sh --bootstrap-server kafka:29092 --topic browser_events
    """
)
```

### Сенсоры (Sensors)

Для ожидания данных использовать `SqlSensor`:

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

wait_for_ods = SqlSensor(
    task_id="wait_for_ods",
    conn_id="clickhouse_default",
    sql="SELECT count() > 0 FROM ods.browser_event",
    mode="poke",
    poke_interval=30,
    timeout=600
)
```

---

## Последовательность внедрения

1. **Этап 1: DDL и Batch**
   - Создать `ddl_init` DAG
   - Создать `etl_batch_transform` DAG
   - Проверить полный цикл: DDL → load (ручной) → transform

2. **Этап 2: Kafka Load**
   - Создать `kafka_load` DAG с параметрами
   - Интегрировать с `etl_batch_transform` через TriggerDagRunOperator

3. **Этап 3: Мониторинг**
   - Добавить `data_quality_monitor` DAG
   - Настроить алерты (email/Slack)

---

## Файловая структура

```
dags/
├── __init__.py
├── ddl_init.py              # DAG #1: Инициализация схемы
├── kafka_load.py            # DAG #2: Загрузка в Kafka
├── etl_batch_transform.py   # DAG #3: Batch ETL
├── data_quality_monitor.py  # DAG #4: DQ мониторинг (опционально)
├── utils/
│   ├── __init__.py
│   ├── clickhouse_helpers.py  # Общие функции для CH
│   └── kafka_helpers.py       # Общие функции для Kafka
└── sql/                     # SQL-шаблоны (опционально)
    ├── dds_click_insert.sql
    ├── dds_event_insert.sql
    └── dm_summary_insert.sql
```

---

## Особенности и ограничения

1. **STG → ODS:** Работает через MV автоматически, не требует DAG.
2. **Очистка Kafka:** MV в ClickHouse запоминают offset'ы — для чистого старта нужно пересоздать MV.
3. **Полная перезагрузка:** Для демо используем `TRUNCATE + INSERT`. В продакшене — инкремент.
4. **Зависимости сервисов:** DAG'и должны проверять доступность ClickHouse/Kafka перед работой.
5. **Идемпотентность:** Batch-задачи должны быть идемпотентны (TRUNCATE перед INSERT).

---

## Проверка после реализации

```bash
# 1. Запуск Airflow
docker compose up -d airflow

# 2. В UI должны появиться DAG'и: ddl_init, kafka_load, etl_batch_transform

# 3. Тестовый прогон:
#    - Trigger ddl_init → проверить таблицы в CH
#    - Trigger kafka_load (limit=50) → проверить топики
#    - Дождаться появления данных в ODS (автоматически через MV)
#    - Trigger etl_batch_transform → проверить DDS и DM

# 4. Проверка результатов:
docker compose exec clickhouse clickhouse-client -q "SELECT * FROM dm.dq_summary"
```
