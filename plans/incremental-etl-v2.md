# План: Инкрементальная загрузка ETL v2

**Дата:** 15 февраля 2026  
**Статус:** Утверждён  
**Цель:** Переход с полной перезагрузки на инкрементальную для учебного стенда

---

## 1. Общие принципы

### 1.1 Watermark
- **Тип:** `event_ts` (бизнес-время события)
- **Хранение:** Таблица `meta.etl_watermarks_history`
- **Гранулярность:** Отдельный watermark на каждую таблицу
- **Lookback:** 5 минут для обработки late arrivals

### 1.2 Обработка ошибок
- При падении загрузки watermark **НЕ обновляется**
- При retry данные дедуплицируются через `ReplacingMergeTree`
- Транзакционность: все слои в рамках одного DAG run

### 1.3 Параллельность
- **ODS слой:** 4 таблицы грузятся **параллельно** (browser, location, device, geo)
- **DDS слой:** Последовательно (зависимости между таблицами)
- **DM слой:** Последовательно

### 1.4 Late Arrivals
- Демонстрация через **код + логи** (без отдельного DAG)
- Комментарии в SQL объясняют логику lookback
- Логирование количества "поздних" событий

---

## 2. Структура хранилища watermark

### 2.1 DDL

```sql
-- meta/etl_watermarks_history.sql
CREATE TABLE IF NOT EXISTS meta.etl_watermarks_history (
    dag_id String,
    dag_run_id String,
    table_name String,
    watermark DateTime64(6),
    rows_processed UInt64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (dag_id, table_name, created_at)
TTL created_at + INTERVAL 30 DAY;  -- Автоудаление старше 30 дней
```

### 2.2 View для получения актуального watermark

```sql
CREATE VIEW IF NOT EXISTS meta.v_latest_watermarks AS
SELECT 
    dag_id,
    table_name,
    argMax(watermark, created_at) as last_watermark,
    argMax(rows_processed, created_at) as last_rows_processed,
    max(created_at) as last_updated
FROM meta.etl_watermarks_history
GROUP BY dag_id, table_name;
```

---

## 3. Модификация SQL загрузок

### 3.1 Шаблон для всех слоёв

Каждый SQL файл получает параметры:
- `{{ dag_id }}` - ID DAG
- `{{ dag_run_id }}` - ID запуска
- `{{ lookback_minutes }}` - 5 (по умолчанию)
- `{{ watermark_table }}` - имя таблицы назначения

### 3.2 STG → ODS (Параллельная загрузка)

**Файлы:**
- `sql/ods/20_stg_to_ods_browser.sql`
- `sql/ods/20_stg_to_ods_location.sql`
- `sql/ods/20_stg_to_ods_device.sql`
- `sql/ods/20_stg_to_ods_geo.sql`

```sql
-- ============================================================================
-- Инкрементальная загрузка STG → ODS (browser_event)
-- 
-- Логика:
-- 1. Получаем последний watermark для таблицы
-- 2. Вычитаем lookback window (для late arrivals)
-- 3. Загружаем данные с event_ts > effective_watermark
-- 4. Дедупликация через ReplacingMergeTree(event_ts)
--
-- Late Arrivals:
-- Если событие пришло с задержкой (например, network latency),
-- оно попадёт в следующую загрузку благодаря lookback.
-- ReplacingMergeTree позаботится о дубликатах.
-- ============================================================================

WITH 
    -- Получаем последний обработанный watermark
    last_watermark AS (
        SELECT COALESCE(
            max(last_watermark), 
            toDateTime64('1970-01-01 00:00:00.000000', 6)
        ) 
        FROM meta.v_latest_watermarks 
        WHERE dag_id = '{{ dag_id }}' 
          AND table_name = '{{ watermark_table }}'
    ),
    
    -- Добавляем lookback window для late arrivals
    -- Это гарантирует, что события с задержкой не потеряются
    effective_watermark AS (
        SELECT last_watermark - INTERVAL {{ lookback_minutes }} MINUTE 
        FROM last_watermark
    ),
    
    -- Считаем сколько "поздних" событий мы обработаем повторно
    stats AS (
        SELECT 
            count() as total_rows,
            countIf(event_ts <= (SELECT * FROM last_watermark)) as late_arrival_rows
        FROM stg.browser_raw
        WHERE event_ts > (SELECT * FROM effective_watermark)
    )

-- Загрузка данных
INSERT INTO ods.browser_event
SELECT 
    event_id,
    click_id,
    user_id,
    event_type,
    event_ts,
    page_url,
    referrer_url,
    -- ... остальные поля
    src_ingest_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset
FROM stg.browser_raw
WHERE event_ts > (SELECT * FROM effective_watermark);

-- Сохраняем watermark только если загрузка успешна
-- Этот INSERT выполняется отдельным task в Airflow
```

### 3.3 ODS → DDS (Последовательная загрузка)

**Файл:** `sql/dds/30_ods_to_dds_incremental.sql`

Аналогичная структура, но:
- Источник: таблицы ODS
- Watermark: `src_ingest_ts`
- Дедупликация: `ReplacingMergeTree(dds_update_ts)`
- Загрузка таблиц последовательно (click → event)

### 3.4 DDS → DM (Последовательная загрузка)

**Файл:** `sql/dm/40_dds_to_dm_incremental.sql`

- Источник: таблицы DDS
- Watermark: `dds_update_ts`
- Витрины пересчитываются за период [watermark - lookback, now]

---

## 4. Модификация Airflow DAG

### 4.1 Параметры

```python
params={
    "mode": Param(
        "incremental", 
        enum=["incremental", "full_refresh"],
        description="Режим загрузки"
    ),
    "lookback_minutes": Param(
        5, 
        type="integer", 
        minimum=0, 
        maximum=60,
        description="Lookback window для late arrivals (минуты)"
    ),
}
```

### 4.2 Структура DAG (псевдокод)

```python
with DAG(...) as dag:
    
    # Task 1: Проверка таблиц
    check_tables = ClickHouseOperator(...)
    
    # Task 2: Получение watermark для всех ODS таблиц
    get_watermark_ods_browser = PythonOperator(..., op_kwargs={'table': 'ods.browser_event'})
    get_watermark_ods_location = PythonOperator(..., op_kwargs={'table': 'ods.location_event'})
    get_watermark_ods_device = PythonOperator(..., op_kwargs={'table': 'ods.device_by_click'})
    get_watermark_ods_geo = PythonOperator(..., op_kwargs={'table': 'ods.geo_by_click'})
    
    # Task 3: Параллельная загрузка STG → ODS
    load_ods_browser = ClickHouseOperator(..., sql='sql/ods/20_stg_to_ods_browser.sql')
    load_ods_location = ClickHouseOperator(..., sql='sql/ods/20_stg_to_ods_location.sql')
    load_ods_device = ClickHouseOperator(..., sql='sql/ods/20_stg_to_ods_device.sql')
    load_ods_geo = ClickHouseOperator(..., sql='sql/ods/20_stg_to_ods_geo.sql')
    
    # Task 4: Сохранение watermark (только при успехе)
    save_watermark_ods_browser = PythonOperator(
        ...,
        trigger_rule='all_success'
    )
    save_watermark_ods_location = PythonOperator(..., trigger_rule='all_success')
    save_watermark_ods_device = PythonOperator(..., trigger_rule='all_success')
    save_watermark_ods_geo = PythonOperator(..., trigger_rule='all_success')
    
    # Task 5: Последовательная загрузка ODS → DDS
    get_watermark_dds = PythonOperator(...)
    load_dds_click = ClickHouseOperator(...)
    save_watermark_dds_click = PythonOperator(..., trigger_rule='all_success')
    load_dds_event = ClickHouseOperator(...)
    save_watermark_dds_event = PythonOperator(..., trigger_rule='all_success')
    
    # Task 6: Загрузка DDS → DM
    get_watermark_dm = PythonOperator(...)
    load_dm = ClickHouseOperator(...)
    save_watermark_dm = PythonOperator(..., trigger_rule='all_success')
    
    # Зависимости
    check_tables >> [get_watermark_ods_browser, get_watermark_ods_location, 
                     get_watermark_ods_device, get_watermark_ods_geo]
    
    get_watermark_ods_browser >> load_ods_browser >> save_watermark_ods_browser
    get_watermark_ods_location >> load_ods_location >> save_watermark_ods_location
    get_watermark_ods_device >> load_ods_device >> save_watermark_ods_device
    get_watermark_ods_geo >> load_ods_geo >> save_watermark_ods_geo
    
    [save_watermark_ods_browser, save_watermark_ods_location,
     save_watermark_ods_device, save_watermark_ods_geo] >> get_watermark_dds
    
    get_watermark_dds >> load_dds_click >> save_watermark_dds_click >> load_dds_event >> save_watermark_dds_event
    
    save_watermark_dds_event >> get_watermark_dm >> load_dm >> save_watermark_dm
```

### 4.3 Функции watermark

```python
def get_watermark(table: str, **context) -> str:
    """
    Получает последний watermark для таблицы.
    Если таблица пустая или watermark не найден - возвращает '1970-01-01'.
    """
    sql = f"""
    SELECT COALESCE(
        max(last_watermark), 
        toDateTime64('1970-01-01 00:00:00.000000', 6)
    ) 
    FROM meta.v_latest_watermarks 
    WHERE dag_id = '{context['dag'].dag_id}' 
      AND table_name = '{table}'
    """
    result = execute_sql(sql)
    watermark = result[0][0] if result else '1970-01-01 00:00:00.000000'
    
    context['ti'].log.info(f"Watermark for {table}: {watermark}")
    return watermark

def save_watermark(table: str, **context):
    """
    Сохраняет watermark после успешной загрузки.
    Выполняется только если upstream task успешен.
    """
    sql = f"""
    SELECT max(event_ts), count() 
    FROM {table}
    WHERE src_ingest_ts > now() - INTERVAL 10 MINUTE
    """
    result = execute_sql(sql)
    watermark, rows = result[0] if result else ('1970-01-01', 0)
    
    insert_sql = f"""
    INSERT INTO meta.etl_watermarks_history 
    (dag_id, dag_run_id, table_name, watermark, rows_processed)
    VALUES 
    ('{context['dag'].dag_id}', '{context['run_id']}', '{table}', '{watermark}', {rows})
    """
    execute_sql(insert_sql)
    
    context['ti'].log.info(
        f"Saved watermark for {table}: {watermark} ({rows} rows)"
    )
```

---

## 5. Алерты (Grafana)

### 5.1 Late Arrivals Alert

**Условие:** Процент late arrivals > 10% за последний час

**PromQL:**
```promql
(
  sum(increase(generator_events_total[1h])) 
  - 
  sum(increase(generator_events_total[1h] offset 5m))
) 
/ 
sum(increase(generator_events_total[1h])) * 100 > 10
```

**Сообщение:** 
"Обнаружено высокое количество late arrivals (>10%). Проверьте задержки в сети или нагрузку на generator."

### 5.2 Stale Watermark Alert

**Условие:** Watermark не обновлялся > 15 минут

**PromQL:**
```promql
time() - max(etl_watermark_timestamp) > 900
```

### 5.3 Ошибки загрузки Alert

**Условие:** Error rate > 0 за последние 5 минут

**PromQL:**
```promql
increase(airflow_dag_run_failures_total{dag_id="etl_pipeline"}[5m]) > 0
```

---

## 6. Демонстрация Late Arrivals

### 6.1 Комментарии в коде

В SQL файлах:
```sql
-- Late Arrivals:
-- Если событие пришло с задержкой (например, network latency),
-- оно попадёт в следующую загрузку благодаря lookback.
-- ReplacingMergeTree позаботится о дубликатах.
```

### 6.2 Логирование

```python
# Логируем количество late arrivals
if late_arrival_rows > 0:
    logger.info(
        f"Processing {late_arrival_rows} late arrival events "
        f"(received after watermark but within lookback window)"
    )
```

### 6.3 Визуализация в Grafana

Добавить панель:
- **Late Arrivals Rate:** Процент событий, обработанных повторно
- **Lookback Efficiency:** Сколько событий в lookback window vs новых

---

## 7. Тестирование

### 7.1 Сценарии

1. **Первый запуск (empty watermark)**
   - Ожидаемое поведение: Загружаются все данные с 1970 года
   - Проверка: `SELECT count() FROM ods.browser_event` > 0

2. **Второй запуск (нет новых данных)**
   - Ожидаемое поведение: Загружено 0 строк
   - Проверка: watermark обновлён, но rows_processed = 0

3. **Late arrival симуляция**
   - Вручную вставить в STG событие с `event_ts` из прошлого
   - Запустить DAG
   - Ожидаемое поведение: Событие загружено (в lookback window)

4. **Ошибка загрузки**
   - Симулировать ошибку (например, неправильный SQL)
   - Ожидаемое поведение: watermark НЕ обновлён
   - Retry должен загрузить те же данные (дедупликация)

### 7.2 Проверочные запросы

```sql
-- Проверить историю watermark
SELECT 
    table_name,
    watermark,
    rows_processed,
    created_at
FROM meta.etl_watermarks_history
ORDER BY created_at DESC
LIMIT 10;

-- Проверить late arrivals
SELECT 
    count() as total_events,
    countIf(event_ts < now() - INTERVAL 5 MINUTE) as late_arrivals
FROM ods.browser_event
WHERE src_ingest_ts > now() - INTERVAL 1 HOUR;
```

---

## 8. Оценка трудозатрат

| Задача | Сложность | Оценка |
|--------|-----------|--------|
| DDL для watermark таблиц | Низкая | 30 мин |
| Модификация SQL ODS (4 файла, параллельно) | Средняя | 3 часа |
| Модификация SQL DDS | Средняя | 2 часа |
| Модификация SQL DM | Средняя | 1.5 часа |
| Рефакторинг DAG (параллельность) | Выше среднего | 4-5 часов |
| Функции watermark + логирование | Низкая | 1 час |
| Алерты Grafana | Низкая | 1 час |
| Тестирование | Средняя | 2 часа |
| **Итого** | | **15-16 часов** |

---

## 9. Следующие шаги

- [ ] Создать ветку `feat/incremental-etl`
- [ ] Создать DDL для watermark таблиц
- [ ] Модифицировать SQL для ODS слоя (4 файла, параллельная загрузка)
- [ ] Модифицировать SQL для DDS слоя
- [ ] Модифицировать SQL для DM слоя
- [ ] Рефакторинг DAG с параллельностью
- [ ] Добавить функции watermark
- [ ] Настроить алерты в Grafana
- [ ] Тестирование
- [ ] Обновление документации
- [ ] Code review
- [ ] Merge в `feature/data-generator`

---

**Согласовано:** _______________  
**Дата:** 15 февраля 2026
