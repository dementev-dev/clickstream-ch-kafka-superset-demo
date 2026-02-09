# Шпаргалка для 5-минутного демо проекта

Цель: за 5 минут показать работодателю рабочий end-to-end пайплайн и инженерный уровень исполнения.

---

## 0) Подготовка до звонка (1 раз)

```bash
make up
docker compose exec -T airflow-webserver airflow dags trigger ddl_init
docker compose exec -T airflow-webserver airflow dags trigger kafka_load --conf '{"limit": 50, "reset_topics": true}'
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline --conf '{"full_refresh": true}'
```

Проверить доступы:
- Airflow: `http://localhost:8080` (`admin/admin`)
- Kafka UI: `http://localhost:8082`
- ClickHouse Play: `http://localhost:9123/play`
- Grafana: `http://localhost:3000`
- Superset: `http://localhost:8088`

---

## 1) Сценарий на 5 минут (тайминг + реплики)

### 0:00–0:30 — Контекст

Что открыть:
- README/схему архитектуры или короткий слайд.

Что сказать:
- «Это мини-DWH кликстрима: Kafka + ClickHouse + Airflow + Superset + Prometheus/Grafana.»
- «Поток: JSONL -> Kafka -> STG -> ODS -> DDS -> DM-витрины.»
- «Ключевая цель: быстрый повторяемый прогон и устойчивость к грязным данным.»

### 0:30–1:20 — Оркестрация в Airflow

Что открыть:
- Airflow UI, DAG-и `ddl_init`, `kafka_load`, `etl_pipeline`.

Что сказать:
- «`ddl_init` создаёт DDL, `kafka_load` грузит данные в Kafka, `etl_pipeline` считает слои.»
- «Запуск ручной, параметры прозрачные: `limit`, `reset_topics`, `full_refresh`.»

### 1:20–2:10 — Ingest через Kafka

Что открыть:
- Kafka UI (топики/сообщения), затем ClickHouse STG.

Что сказать:
- «Одна строка входа = одно сообщение Kafka.»
- «В STG храним сырой JSON без потери данных, типизация делается позже в ODS.»

### 2:10–3:20 — Проверка результата в ClickHouse

Что открыть:
- ClickHouse Play и выполнить запросы ниже.

Что сказать:
- «Показываю факт прохождения по слоям и готовые витрины для аналитики.»

```sql
SELECT count() AS rows FROM ods.browser_event;
SELECT count() AS rows FROM dds.event;
SELECT count() AS rows FROM dds.click;
SELECT * FROM dm.v_daily_traffic ORDER BY event_date DESC LIMIT 10;
SELECT * FROM dm.v_utm_effectiveness ORDER BY clicks DESC LIMIT 10;
```

### 3:20–4:10 — Качество данных и устойчивость

Что открыть:
- `dm.dq_summary` и/или `ods.*_errors`.

Что сказать:
- «Грязные записи не валят пайплайн: ошибки фиксируются в ODS и отражаются в DQ summary.»
- «Это важнее “идеально чистого” датасета, потому что поведение ближе к прод-среде.»

```sql
SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name LIMIT 20;
```

### 4:10–5:00 — Мониторинг и BI

Что открыть:
- Grafana (overview-дашборд) и Superset (одна витрина/чарт).

Что сказать:
- «Есть observability: метрики по ClickHouse/Kafka/Airflow и алерты.»
- «Есть BI-слой: витрины готовы для первичного анализа без ручных выгрузок.»

---

## 2) План Б, если UI тормозит

Показать то же самое через CLI:

```bash
docker compose ps
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.event"
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT * FROM dm.dq_summary LIMIT 10"
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

Короткая реплика:
- «Даже без UI видно, что pipeline отработал, витрины заполнены, мониторинг жив.»

---

## 3) Финальная фраза (15 секунд)

- «Проект показывает полный цикл DE-задачи: инфраструктура, ingestion, слои данных, DQ, витрины и мониторинг.»
- «Если нужно, могу углубиться в любой блок: DAG, SQL-трансформации, модель данных или алерты.»
