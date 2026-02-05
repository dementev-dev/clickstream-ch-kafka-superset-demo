# AGENTS.md

Инструкции для работы с репозиторием мини‑демо хранилища кликстрима.

## Цель репозитория

Развернуть в `docker compose` минимальный аналитический стек:

- Kafka (источник событий, 1 JSON message = 1 event)
- ClickHouse (STG → ODS → DDS → DM)
- Superset (BI поверх витрин)
- Prometheus + Grafana (мониторинг)
- простой инструмент/скрипт, который читает `.jsonl` и пишет события в Kafka

## Ключевые артефакты

- `plans/clickhouse_ddl.md` — основной документ со схемой слоёв, DDL/MV и витринами DM.
- `plans/runbook.md` — runbook: порядок запуска (`make up`/`make ddl`/`make data`) и параметры.
- `plans/kafka_ingest_plan.md` — детальный план реализации загрузки данных в Kafka.
- `data/DE-task.md` — текст задания.
- `data/*.jsonl` — исходные данные (могут быть грязными).
- `configs/` — конфиги ClickHouse/Prometheus/Grafana (по мере развития).

## Правила по данным (важно)

- Не загружать исходные `*.jsonl` целиком: используйте `head -n 20..50`.
- Для тестов/демо предпочтительнее “малый срез”, чем “идеальная полнота”.
- Данные могут быть с ошибками — пайплайн должен быть устойчивым (в ODS фиксировать ошибки парсинга, а не падать).

## Как запускать (локально)

Базовые команды:

- `make up` (или `docker compose up -d`)
- `make ddl` (применяет SQL из `plans/clickhouse_ddl.md` в ClickHouse)
- `make data` (пересоздаёт топики и заливает небольшой срез данных в Kafka; полный режим — `FULL=1 make data`)
- `docker compose up -d`
- `docker compose ps`
- `docker compose logs -f --tail=200 <service>`
- `docker compose down -v` (удалит volumes; используйте осознанно)

Порты (см. `docker-compose.yml`):

- ClickHouse native: `localhost:8002`
- ClickHouse HTTP: `localhost:9123`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8082`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## ClickHouse: применение DDL и загрузка сэмпла

- DDL/пайплайн описаны в `plans/clickhouse_ddl.md`.
- Для быстрой загрузки “первых N строк” используйте команды из раздела “Практические заметки для демо”.

## Конвенции по изменениям

- Держать изменения минимальными и по теме задания (инфра, схема, ingest, витрины).
- Не коммитить секреты. Если требуется пароль/ключи — использовать `.env` и примеры `.env.example`.
- README/планы обновлять вместе с изменениями инфраструктуры/DDL.

## Быстрые проверки

- Kafka ingest: наличие данных в `stg.*` и типизированных строк в `ods.*`.
- Мониторинг: доступность `/metrics` у ClickHouse и скрейп в Prometheus.
- BI: витрина `dm.v_events_enriched` должна отвечать за разумное время при фильтре по дате.

## Примечания по текущему состоянию (если что-то “не встаёт”)

Репозиторий развивается итеративно; если `docker compose` не стартует из‑за отсутствующих путей/сетей/сервисов, правьте аккуратно и фиксируйте это в `docker-compose.yml` и/или `configs/`.
