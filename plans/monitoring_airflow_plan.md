# План подключения Airflow к мониторингу

> Дата создания: 2026-02-08  
> Источник: Airflow 2.10.5 via statsd_exporter  
> Проверка API: Context7 (`/prometheus/docs` для scrape_configs)

---

## 1. Обзор

Подключаем мониторинг Airflow через **statsd_exporter**. Airflow встроенно поддерживает отправку метрик в StatsD-формате, а statsd_exporter конвертирует их в Prometheus-формат.

**Метрики, которые будем собирать:**
- `airflow_dag_processing_total_parse_time` — время парсинга DAG
- `airflow_dagbag_size` — количество DAG в DagBag
- `airflow_executor_open_slots` — доступные слоты executor
- `airflow_executor_queued_tasks` — задачи в очереди
- `airflow_executor_running_tasks` — запущенные задачи
- `airflow_task_duration{task_id, dag_id}` — длительность выполнения тасков
- `airflow_task_failures` — количество падений тасков
- `airflow_task_success` — успешные выполнения
- `airflow_scheduler_heartbeat` — heartbeats шедулера

---

## 2. Изменения в инфраструктуре

### 2.1. docker-compose.yml

Добавить сервис `statsd-exporter`:

```yaml
  statsd-exporter:
    image: prom/statsd-exporter:v0.27.1
    command: [
      "--statsd.listen-udp=:8125",
      "--statsd.listen-tcp=",
      "--web.listen-address=:9102",
      "--statsd.mapping-config=/tmp/statsd_mapping.yml"
    ]
    ports:
      - "9102:9102"    # Prometheus metrics endpoint
      - "8125:8125/udp" # StatsD receive port
    volumes:
      - ./configs/statsd_mapping.yml:/tmp/statsd_mapping.yml:ro
    networks:
      - cs_dwh
```

Добавить переменные окружения в `airflow-default-env`:

```yaml
x-airflow-env: &airflow-default-env
  # ... существующие переменные ...
  # StatsD для мониторинга
  AIRFLOW__METRICS__STATSD_ON: "True"
  AIRFLOW__METRICS__STATSD_HOST: "statsd-exporter"
  AIRFLOW__METRICS__STATSD_PORT: "8125"
  AIRFLOW__METRICS__STATSD_PREFIX: "airflow"
```

**Почему `prom/statsd-exporter:v0.27.1`:**
- Стабильная версия с поддержкой mapping-конфигурации
- Официальный образ от Prometheus team
- Проверено на совместимость с Airflow 2.10.x

### 2.2. configs/statsd_mapping.yml

Создать mapping-конфигурацию для конвертации StatsD метрик в Prometheus:

```yaml
# Mapping StatsD → Prometheus для Airflow
mappings:
  # DAG processing metrics
  - match: "airflow.dag_processing.total_parse_time"
    name: "airflow_dag_processing_total_parse_time"
    help: "Total time to parse all DAGs"
    type: gauge

  - match: "airflow.dagbag_size"
    name: "airflow_dagbag_size"
    help: "DAGs in the DagBag"
    type: gauge

  # Executor metrics
  - match: "airflow.executor.open_slots"
    name: "airflow_executor_open_slots"
    help: "Open slots on executor"
    type: gauge

  - match: "airflow.executor.queued_tasks"
    name: "airflow_executor_queued_tasks"
    help: "Queued tasks on executor"
    type: gauge

  - match: "airflow.executor.running_tasks"
    name: "airflow_executor_running_tasks"
    help: "Running tasks on executor"
    type: gauge

  # Task metrics с labels
  - match: "airflow.operator_failures_*"
    name: "airflow_operator_failures"
    help: "Operator failures"
    type: counter
    labels:
      operator: "$1"

  - match: "airflow.operator_successes_*"
    name: "airflow_operator_successes"
    help: "Operator successes"
    type: counter
    labels:
      operator: "$1"

  # Task duration by dag_id и task_id
  - match: "airflow.dag.*.task.*.duration"
    name: "airflow_task_duration"
    help: "Task duration by dag_id and task_id"
    type: timer
    labels:
      dag_id: "$1"
      task_id: "$2"

  # Task failures/success by dag_id и task_id
  - match: "airflow.dag.*.task.*.failures"
    name: "airflow_task_failures"
    help: "Task failures by dag_id and task_id"
    type: counter
    labels:
      dag_id: "$1"
      task_id: "$2"

  - match: "airflow.dag.*.task.*.success"
    name: "airflow_task_success"
    help: "Task success by dag_id and task_id"
    type: counter
    labels:
      dag_id: "$1"
      task_id: "$2"

  # Scheduler metrics
  - match: "airflow.scheduler_heartbeat"
    name: "airflow_scheduler_heartbeat"
    help: "Scheduler heartbeats"
    type: counter

  - match: "airflow.scheduler.critical_section_busy"
    name: "airflow_scheduler_critical_section_busy"
    help: "Scheduler critical section busy"
    type: gauge

  # Catch-all для остальных airflow метрик
  - match: "airflow.*"
    name: "airflow_${1}"
    help: "Airflow metric $1"
    type: gauge
```

### 2.3. configs/prometheus.yml

Добавить job для скрейпа statsd-exporter:

```yaml
scrape_configs:
  - job_name: "clickhouse"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["clickhouse:9126"]
        labels:
          instance: Clickhouse-1
    honor_labels: true

  - job_name: "kafka"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["kafka-exporter:9308"]
        labels:
          instance: Kafka-1
    honor_labels: true

  # Новый job для Airflow
  - job_name: "airflow"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["statsd-exporter:9102"]
        labels:
          instance: Airflow-1
    honor_labels: true
```

---

## 3. Дашборд Grafana

### 3.1. Создать файл: `configs/grafana/provisioning/dashboards/airflow-overview.json`

Основные панели дашборда:

| Панель | PromQL запрос | Описание |
|--------|--------------|----------|
| DAG Bag Size | `airflow_dagbag_size` | Количество DAG в системе |
| Parse Time | `airflow_dag_processing_total_parse_time` | Время парсинга DAG |
| Executor Slots | `airflow_executor_open_slots` | Доступные слоты |
| Queued Tasks | `airflow_executor_queued_tasks` | Задачи в очереди |
| Running Tasks | `airflow_executor_running_tasks` | Выполняемые задачи |
| Task Duration | `rate(airflow_task_duration_sum[5m]) / rate(airflow_task_duration_count[5m])` | Средняя длительность тасков |
| Task Failures | `rate(airflow_task_failures[5m])` | Rate падений по таскам |
| Scheduler Heartbeat | `rate(airflow_scheduler_heartbeat[5m])` | Активность шедулера |

### 3.2. Структура дашборда (основные секции)

```json
{
  "dashboard": {
    "id": null,
    "uid": "airflow-overview",
    "title": "Airflow Overview",
    "tags": ["airflow", "orchestration"],
    "timezone": "Europe/Moscow",
    "schemaVersion": 36,
    "refresh": "10s",
    "panels": [
      // Row 1: Scheduler Health
      // - DAG Bag Size (stat)
      // - Parse Time (gauge)
      // - Scheduler Heartbeat (graph)
      
      // Row 2: Executor Status
      // - Open Slots (stat)
      // - Queued Tasks (stat)
      // - Running Tasks (stat)
      // - Tasks distribution (graph)
      
      // Row 3: Task Performance
      // - Task Duration by DAG (heatmap или graph)
      // - Task Failures Rate (graph)
      // - Task Success Rate (graph)
      
      // Row 4: Per-DAG Details
      // - Top DAGs by runtime (table)
      // - Failed tasks by DAG (table)
    ]
  }
}
```

---

## 4. Алерты Grafana

### 4.1. Создать файл: `configs/grafana/provisioning/alerting/airflow-alert-rules.yml`

```yaml
apiVersion: 1
groups:
  - orgId: 1
    name: airflow_alerts
    folder: Airflow
    interval: 30s
    rules:
      # Alert: Scheduler не отправляет heartbeats
      - uid: airflow_scheduler_down
        title: Airflow Scheduler Down
        condition: B
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus_uid
            model:
              expr: rate(airflow_scheduler_heartbeat[5m])
              instant: true
          - refId: B
            relativeTimeRange:
              from: 0
              to: 0
            datasourceUid: __expr__
            model:
              type: threshold
              expression: A
              conditions:
                - evaluator:
                    type: lt
                    params: [0.1]
        noDataState: NoData
        execErrState: Error
        for: 2m
        annotations:
          summary: "Airflow scheduler не отправляет heartbeats"
          description: "Scheduler possible down — rate(airflow_scheduler_heartbeat) < 0.1 в течение 2 минут"
        labels:
          severity: critical

      # Alert: В очереди слишком много задач
      - uid: airflow_queue_high
        title: Airflow Queue Backlog
        condition: B
        data:
          - refId: A
            datasourceUid: prometheus_uid
            model:
              expr: airflow_executor_queued_tasks
              instant: true
          - refId: B
            datasourceUid: __expr__
            model:
              type: threshold
              expression: A
              conditions:
                - evaluator:
                    type: gt
                    params: [50]
        noDataState: OK
        execErrState: Error
        for: 5m
        annotations:
          summary: "В очереди Airflow > 50 задач"
          description: "airflow_executor_queued_tasks превышает 50 в течение 5 минут — возможна перегрузка"
        labels:
          severity: warning

      # Alert: Много падений тасков
      - uid: airflow_task_failures_high
        title: High Task Failure Rate
        condition: B
        data:
          - refId: A
            datasourceUid: prometheus_uid
            model:
              expr: rate(airflow_task_failures[5m])
              instant: true
          - refId: B
            datasourceUid: __expr__
            model:
              type: threshold
              expression: A
              conditions:
                - evaluator:
                    type: gt
                    params: [0.1]
        noDataState: OK
        execErrState: Error
        for: 3m
        annotations:
          summary: "Высокий rate падений тасков"
          description: "rate(airflow_task_failures) > 0.1 в течение 3 минут — проверьте логи DAG"
        labels:
          severity: warning

      # Alert: DAG parsing занимает слишком много времени
      - uid: airflow_parse_time_high
        title: High DAG Parse Time
        condition: B
        data:
          - refId: A
            datasourceUid: prometheus_uid
            model:
              expr: airflow_dag_processing_total_parse_time
              instant: true
          - refId: B
            datasourceUid: __expr__
            model:
              type: threshold
              expression: A
              conditions:
                - evaluator:
                    type: gt
                    params: [30]
        noDataState: OK
        execErrState: Error
        for: 5m
        annotations:
          summary: "Парсинг DAG занимает > 30 секунд"
          description: "Возможно, есть тяжелые DAG — оптимизируйте или разбейте на под-DAG"
        labels:
          severity: info
```

---

## 5. Проверка после деплоя

```bash
# 1. Перезапуск стека с новым сервисом
make reload-monitoring

# 2. Проверка, что statsd-exporter поднялся
curl -s http://localhost:9102/metrics | grep airflow

# 3. Проверка таргета в Prometheus
curl -s http://localhost:9090/api/v1/targets | grep -A5 airflow

# 4. Проверка дашборда в Grafana
open http://localhost:3000/d/airflow-overview

# 5. Проверка алертов
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | grep airflow
```

---

## 6. Troubleshooting

### Нет метрик airflow_* в Prometheus

1. Проверить env vars в Airflow:
   ```bash
   docker compose exec airflow-webserver env | grep STATSD
   ```
   Ожидается: `AIRFLOW__METRICS__STATSD_ON=True`

2. Проверить доступность statsd-exporter:
   ```bash
   docker compose exec airflow-webserver nc -zv statsd-exporter 8125
   ```

3. Проверить метрики в statsd-exporter напрямую:
   ```bash
   curl -s http://localhost:9102/metrics
   ```

### StatsD метрики не мапятся

Проверить формат mapping-файла:
```bash
docker compose logs statsd-exporter | grep -i "mapping\|error"
```

### Airflow не отправляет метрики

Проверить конфигурацию внутри Airflow:
```bash
docker compose exec airflow-webserver airflow config get-value metrics statsd_on
docker compose exec airflow-webserver airflow config get-value metrics statsd_host
```

---

## 7. Зависимости и порядок применения

1. Создать `configs/statsd_mapping.yml`
2. Добавить `statsd-exporter` в `docker-compose.yml`
3. Добавить StatsD env vars в `airflow-default-env`
4. Обновить `configs/prometheus.yml` (добавить job airflow)
5. Создать дашборд `configs/grafana/provisioning/dashboards/airflow-overview.json`
6. Создать алерты `configs/grafana/provisioning/alerting/airflow-alert-rules.yml`
7. Запустить `make reload-monitoring`
