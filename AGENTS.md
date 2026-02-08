# AGENTS.md

Инструкции для работы с репозиторием мини‑демо хранилища кликстрима.

## Цель репозитория

Решение [тестового задания DE](./data/DE-task.md) — развернуть в `docker compose` минимальный аналитический стек для обработки кликстрима e-commerce:

**Задача:** Подготовить данные для первичного анализа и собрать дашборд, на котором бизнес может сделать выводы.

**Итоговый стек:**

- Kafka (источник событий, 1 JSON message = 1 event)
- ClickHouse (STG → ODS → DDS → DM)
- **Airflow** (оркестрация ETL-пайплайна)
- Superset (BI поверх витрин)
- Prometheus + Grafana (мониторинг)
- простой инструмент/скрипт, который читает `.jsonl` и пишет события в Kafka

## Ключевые артефакты

### Исполняемые файлы (текущая структура)
- `dags/` — Airflow DAGs для оркестрации ETL
- `sql/` — SQL по слоям:
  - `sql/ddl/00_databases.sql` — создание БД stg/ods/dds/dm
  - `sql/ddl/stg/10_stg.sql` — STG слой (Kafka Engine + MV)
  - `sql/ddl/ods/20_ods.sql` — ODS слой (типизация + MV для ошибок)
  - `sql/ddl/dds/30_dds.sql` — DDS слой (таблицы для batch-загрузки)
  - `sql/ddl/dm/40_dm.sql` — DM слой (витрины VIEW)
  - `sql/dds/30_ods_to_dds.sql` — ODS → DDS (argMax + JOIN)
  - `sql/dm/40_dds_to_dm.sql` — обновление DQ_summary
- `scripts/` — скрипты автоматизации:
  - `apply_clickhouse_ddl.sh` — применение DDL
  - `load_kafka_data.sh` — загрузка в Kafka
  - `run_batch.sh` — запуск batch-процесса
- `airflow/` — конфигурация Airflow:
  - `requirements.txt` — зависимости Airflow/ClickHouse plugin

### Планы и документация (legacy)
- `plans/clickhouse_ddl.md` — исходный план (inline DDL, legacy)
- `plans/runbook.md` — runbook
- `plans/kafka_ingest_plan.md` — план загрузки в Kafka
- `docs/ARCHITECTURE.md` — подробное описание архитектуры
- `data/DE-task.md` — текст задания.
- `data/*.jsonl` — исходные данные (могут быть грязными).
- `configs/` — конфиги ClickHouse/Prometheus/Grafana.

## Правила по данным (важно)

- Не загружать исходные `*.jsonl` целиком: используйте `head -n 20..50`.
- Для тестов/демо предпочтительнее “малый срез”, чем “идеальная полнота”.
- Данные могут быть с ошибками — пайплайн должен быть устойчивым (в ODS фиксировать ошибки парсинга, а не падать).

## Как запускать (локально)

Базовые команды:

- `make up` (или `docker compose up -d`)
- `make ddl` (применяет SQL из `sql/ddl/00_databases.sql` и `sql/ddl/*/*.sql` в ClickHouse)
- `make data` (пересоздаёт топики и заливает небольшой срез данных в Kafka; полный режим — `FULL=1 make data`)
- `make transform` (запускает batch-процесс ODS → DDS → DM)
- `docker compose up -d`
- `docker compose ps`
- `docker compose logs -f --tail=200 <service>`
- `docker compose down` (сохраняет named volumes, включая `clickhouse-data`)
- `docker compose down -v` (удалит volumes; используйте осознанно)

Порты (см. `docker-compose.yml`):

- ClickHouse native: `localhost:8002`
- ClickHouse HTTP: `localhost:9123`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8082`
- **Airflow: `http://localhost:8080` (admin/admin)**
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## ClickHouse: применение DDL и загрузка сэмпла

- DDL/пайплайн описаны в `plans/clickhouse_ddl.md`.
- Для быстрой загрузки “первых N строк” используйте команды из раздела “Практические заметки для демо”.

## Конвенции по изменениям

- Держать изменения минимальными и по теме задания (инфра, схема, ingest, витрины).
- Не коммитить секреты. Если требуется пароль/ключи — использовать `.env` и примеры `.env.example`.
- Оформлять коммиты по правилам из [COMMIT_RULES.md](./docs/COMMIT_RULES.md).
- README/планы обновлять вместе с изменениями инфраструктуры/DDL.
- Для спорных или меняющихся API (особенно Airflow/operators/providers) проверять актуальную документацию через `context7` и фиксировать решение в коде/документации.
- **Комментарии в коде — на русском языке**:
  - SQL: заголовочный блок с описанием файла, комментарии к каждому логическому блоку
  - Bash: шапка с назначением/запуском/требованиями, секции разделены `# -----`
  - См. существующие файлы как пример (`sql/ddl/ods/20_ods.sql`, `sql/dds/30_ods_to_dds.sql`, `scripts/run_batch.sh`)

## Быстрые проверки

- Kafka ingest: наличие данных в `stg.*` и типизированных строк в `ods.*`.
- Мониторинг: доступность `/metrics` у ClickHouse и скрейп в Prometheus.
- **Airflow: `http://localhost:8080` должен показывать UI и DAG `ddl_init` и `etl_pipeline`.**
- BI: витрина `dm.v_events_enriched` должна отвечать за разумное время при фильтре по дате.

## Связанная документация

- [README.md](./README.md) — пользовательская документация (быстрый старт, архитектура)
- [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) — подробное описание слоёв и технических решений
- [data/DE-task.md](./data/DE-task.md) — исходное задание
- [COMMIT_RULES.md](./docs/COMMIT_RULES.md) — правила оформления коммитов

## Примечания по текущему состоянию (если что-то “не встаёт”)

Репозиторий развивается итеративно; если `docker compose` не стартует из‑за отсутствующих путей/сетей/сервисов, правьте аккуратно и фиксируйте это в `docker-compose.yml` и/или `configs/`.
