# Сценарий демо на 10-15 минут (код + архитектура)

Цель: дать студенту готовый сценарий, который показывает не только запуск стенда, но и инженерные решения в коде.

Формат: запись экрана + голос.  
Ориентир по времени: 12 минут (допуск 10-15).

---

## 1) Подготовка перед записью (за 10-20 минут)

```bash
make up
docker compose ps
docker compose exec -T airflow-webserver airflow dags trigger ddl_init
docker compose exec -T airflow-webserver airflow dags trigger kafka_load --conf '{"limit": 50, "reset_topics": true}'
docker compose exec -T airflow-webserver airflow dags trigger etl_pipeline --conf '{"full_refresh": true}'
```

Проверить, что открываются:
- Kafka UI: `http://localhost:8082`
- Grafana: `http://localhost:3000/d/clickhouse-overview/clickhouse-overview`
- Airflow: `http://localhost:8080/dags/ddl_init/grid?tab=details`
- Superset (если используете): `http://localhost:8088/login/?next=/`
- ClickHouse Play: `http://localhost:9123/play`

Подготовить вкладки заранее:
- `Makefile`
- `docker-compose.yml`
- `docs/ARCHITECTURE.md`
- `airflow/dags/ddl_init_dag.py`
- `airflow/dags/kafka_load_dag.py`
- `airflow/dags/etl_pipeline_dag.py`
- `sql/ddl/stg/10_stg.sql`
- `sql/ods/20_stg_to_ods.sql`
- `sql/dds/30_ods_to_dds.sql`
- `sql/dm/40_dds_to_dm.sql`

Опционально подготовить DBeaver (если хотите показывать не через Play):
- Host: `localhost`
- Port: `9123` (HTTP) или `8002` (native)
- User: `default`
- Password: `123456`

---

## 2) Поминутный план выступления

### 0:00-1:30 Инфраструктура и цель проекта

Что показывать:
- Терминал с `docker compose ps`
- `Makefile`
- `docker-compose.yml`

Что говорить:
- «Это учебный mini DWH для кликстрима: Kafka, ClickHouse, Airflow, Superset, Prometheus, Grafana.»
- «Инфраструктура поднимается одной командой `make up`; внутри это `docker compose up -d`.»
- «В `Makefile` также есть команды для остановки, очистки, перезагрузки мониторинга и recovery.»
- «Сервисная цель проекта: быстро и повторяемо показать end-to-end поток данных до витрин.»

Что подчеркнуть в коде:
- В `Makefile` показать цели `up/down/clean/reload-monitoring/recover-monitoring`.
- В `docker-compose.yml` бегло показать ключевые сервисы и порты.

### 1:30-3:30 Архитектура и логика выбора

Что показывать:
- `docs/ARCHITECTURE.md` (диаграммы потока, слои STG/ODS/DDS/DM).

Что говорить:
- «Управление сделано через 3 DAG: `ddl_init`, `kafka_load`, `etl_pipeline`.»
- «STG нужен для сырых событий как есть, чтобы сохранять воспроизводимость.»
- «ODS типизирует и валидирует данные, включая фиксацию ошибок парсинга.»
- «DDS собирает бизнес-сущности `event` и `click` для аналитики.»
- «DM отдает витрины и агрегаты для BI и интервью-демо.»

Объяснение решений:
- «Разделение на слои уменьшает связность и ускоряет диагностику проблем.»
- «Грязные данные не останавливают пайплайн: ошибки уходят в `ods.*_errors` и DQ-слой.»

### 3:30-6:30 Показ кода DAG-ов

Что показывать:
- `airflow/dags/ddl_init_dag.py`
- `airflow/dags/kafka_load_dag.py`
- `airflow/dags/etl_pipeline_dag.py`

Что говорить:
- «В `ddl_init` код разворачивает DDL в ClickHouse и подготавливает структуру слоев.»
- «В `kafka_load` есть управляемые параметры `limit` и `reset_topics` для быстрого smoke-прогона.»
- «В `etl_pipeline` выполняются шаги STG->ODS->DDS->DM с конфигурацией `full_refresh`; итоговый DM-блок здесь — загрузка `dm.dq_summary`.»
- «Логика запуска ручная: это удобно для демонстрации на собеседовании и для отладки.»

Что обязательно назвать:
- «Почему `limit=50` в демо: скорость и повторяемость важнее полноты.»
- «Почему DAG-и разделены: проще локализовать сбой и перезапустить только нужный этап.»

### 6:30-8:30 Показ SQL и модели данных

Что показывать:
- `sql/ddl/stg/10_stg.sql`
- `sql/ods/20_stg_to_ods.sql`
- `sql/dds/30_ods_to_dds.sql`
- `sql/dm/40_dds_to_dm.sql`

Что говорить:
- «В STG используется связка Kafka Engine + Materialized View + MergeTree таблицы.»
- «ODS делает типизацию, нормализацию и отправку проблемных строк в таблицы ошибок.»
- «DDS собирает сущности по ключам (`event_id`, `click_id`), чтобы упростить аналитику.»
- «Витрины DM строятся поверх DDS и готовы для BI.»

Короткий акцент на DQ:
- «Вместо падения на невалидном JSON сохраняем ошибку и продолжаем обработку потока.»

### 8:30-10:30 Прогон в Airflow + проверка результата

Что показывать:
- Airflow UI: последний `Success` у `ddl_init`, `kafka_load`, `etl_pipeline`
- ClickHouse Play или DBeaver

Что выполнять:

```sql
SELECT count() AS rows FROM stg.browser_raw;
SELECT count() AS rows FROM ods.browser_event;
SELECT count() AS rows FROM dds.event;
SELECT * FROM dm.v_daily_traffic ORDER BY event_date DESC LIMIT 10;
SELECT * FROM dm.dq_summary ORDER BY layer, table_name, check_name LIMIT 20;
```

Что говорить:
- «На экране видно прохождение данных по слоям и непустые витрины.»
- «DQ summary подтверждает контроль качества и обработку проблемных записей.»

### 10:30-12:00 Мониторинг и финал

Что показывать:
- Grafana: ClickHouse/Kafka/Airflow dashboards
- Prometheus targets (опционально)
- Superset dashboard (если подготовлен)

Что говорить:
- «Мониторинг показывает здоровье стенда и ключевые технические метрики.»
- «На BI-слое уже можно отвечать на базовые бизнес-вопросы по трафику и UTM.»
- «Итог: решение покрывает инфраструктуру, ingestion, трансформации, DQ, витрины и observability.»

---

## 3) План Б, если что-то сломалось на записи

Если не открывается UI:

```bash
docker compose ps
docker compose logs -f --tail=100 airflow-webserver
docker compose exec -T clickhouse clickhouse-client --user=default --password=123456 --query "SELECT count() FROM dds.event"
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

Если Grafana пустая:

```bash
make reload-monitoring
```

Если мониторинг завис:

```bash
make recover-monitoring
```

Короткая реплика:
- «Даже при проблемах UI я показываю проверку через CLI и SQL, чтобы подтвердить работоспособность пайплайна.»

---

## 4) Готовый текст финала (20-30 секунд)

«Я реализовал end-to-end mini DWH для кликстрима: от инфраструктуры и ingestion до витрин и мониторинга.  
Архитектура послойная STG-ODS-DDS-DM, orchestration через Airflow DAG-и, а невалидные данные фиксируются без падения пайплайна.  
Если нужно, могу детально разобрать любой уровень: DAG-код, SQL-трансформации, DQ-проверки или наблюдаемость системы.»
