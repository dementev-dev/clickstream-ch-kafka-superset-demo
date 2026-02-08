# План подключения Kafka к мониторингу

> Дата создания: 2026-02-08  
> Источник: Kafka via danielqsj/kafka-exporter  
> Проверка API: Context7 (`/danielqsj/kafka_exporter`, `/prometheus/docs`)

---

## 1. Обзор

Подключаем мониторинг Kafka через **kafka-exporter** (Prometheus exporter для Kafka). Используем официальный образ `danielqsj/kafka-exporter:latest`.

**Метрики, которые будем собирать:**
- `kafka_brokers` — количество брокеров в кластере
- `kafka_topic_partitions` — количество партиций по топикам
- `kafka_topic_partition_current_offset` — текущий offset
- `kafka_topic_partition_oldest_offset` — oldest offset
- `kafka_consumer_group_lag` — лаг консьюмер-групп
- `kafka_consumer_group_current_offset` — текущий offset консьюмеров

---

## 2. Изменения в инфраструктуре

### 2.1. docker-compose.yml

Добавить сервис `kafka-exporter`:

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    command: ["--kafka.server=kafka:29092"]
    ports:
      - "9308:9308"
    networks:
      - cs_dwh
    depends_on:
      - kafka
```

**Почему `kafka:29092`:** внутри Docker-сети Kafka слушает на `PLAINTEXT://kafka:29092` (см. `KAFKA_ADVERTISED_LISTENERS` в текущем docker-compose.yml).

### 2.2. configs/prometheus.yml

Добавить job для скрейпа kafka-exporter:

```yaml
scrape_configs:
  - job_name: "clickhouse"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["clickhouse:9126"]
        labels:
          instance: Clickhouse-1
    honor_labels: true

  # Новый job для Kafka
  - job_name: "kafka"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["kafka-exporter:9308"]
        labels:
          instance: Kafka-1
    honor_labels: true
```

**Проверка через Context7:** формат `scrape_configs` подтверждён через `/prometheus/docs` (snippet: `scrape_configs: - job_name: myapp ... static_configs: - targets:`).

---

## 3. Дашборд Grafana

### 3.1. Создать файл: `configs/grafana/provisioning/dashboards/kafka-overview.json`

Основные панели дашборда:

| Панель | PromQL запрос | Описание |
|--------|--------------|----------|
| Brokers Up | `kafka_brokers` | Количество доступных брокеров |
| Topics Count | `count(kafka_topic_partitions) by (topic)` | Количество топиков |
| Total Partitions | `sum(kafka_topic_partitions)` | Всего партиций |
| Messages In/sec | `rate(kafka_topic_partition_current_offset[5m])` | Скорость записи сообщений |
| Consumer Lag | `kafka_consumer_group_lag` | Лаг по консьюмер-группам |
| Partition Offsets | `kafka_topic_partition_current_offset` | Текущие offsets по партициям |

### 3.2. Структура дашборда (JSON)

```json
{
  "dashboard": {
    "id": null,
    "uid": "kafka-overview",
    "title": "Kafka Overview",
    "tags": ["kafka", "messaging"],
    "timezone": "Europe/Moscow",
    "schemaVersion": 36,
    "refresh": "10s",
    "panels": [
      // Row 1: Cluster Health
      // - Brokers Up (stat panel)
      // - Topics Count (stat panel)
      // - Total Partitions (stat panel)
      
      // Row 2: Throughput
      // - Messages In/sec (graph panel)
      // - Partition Offsets (graph panel)
      
      // Row 3: Consumers
      // - Consumer Lag (table panel)
      // - Consumer Offsets (graph panel)
    ]
  }
}
```

---

## 4. Алерт-правила Grafana

### 4.1. Создать/дополнить: `configs/grafana/provisioning/alerting/kafka-alert-rules.yml`

```yaml
apiVersion: 1

groups:
  - orgId: 1
    name: kafka_health_group
    folder: Kafka Alerts
    interval: 1m
    rules:
      # Alert: Kafka broker down
      - uid: kafka_broker_down
        title: Kafka Broker Down
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus_uid
            model:
              expr: kafka_brokers < 1
        noDataState: Alerting
        for: 1m
        annotations:
          summary: "Kafka broker недоступен"

      # Alert: High consumer lag
      - uid: kafka_consumer_lag_high
        title: Kafka Consumer Lag High
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus_uid
            model:
              expr: kafka_consumer_group_lag > 10000
        for: 5m
        annotations:
          summary: "Высокий лаг консьюмера {{ $labels.group }} для топика {{ $labels.topic }}"
```

---

## 5. Порядок внедрения

```bash
# 1. Добавить сервис в docker-compose.yml
# 2. Обновить configs/prometheus.yml
# 3. Создать дашборд configs/grafana/provisioning/dashboards/kafka-overview.json
# 4. Создать алерты configs/grafana/provisioning/alerting/kafka-alert-rules.yml
# 5. Применить изменения

docker compose up -d kafka-exporter
docker compose restart prometheus

# Перезагрузить provisioning Grafana
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload

# Проверка
curl -s http://localhost:9090/api/v1/targets | grep kafka
curl -s http://localhost:9308/metrics | head -20
```

---

## 6. URL после внедрения

| Сервис | URL |
|--------|-----|
| Kafka Exporter metrics | http://localhost:9308/metrics |
| Grafana Dashboard | http://localhost:3000/d/kafka-overview/kafka-overview |
| Prometheus Targets | http://localhost:9090/targets |

---

## 7. Проверка метрик

```bash
# Проверить, что kafka-exporter отдаёт метрики
curl -s http://localhost:9308/metrics | grep "^kafka_"

# Проверить, что Prometheus собирает метрики
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers"
```

---

## 8. Troubleshooting

| Проблема | Решение |
|----------|---------|
| `connection refused` к Kafka | Проверить, что используется `kafka:29092` (внутренняя сеть), не `localhost:9092` |
| `No data` в Grafana | Проверить Status -> Targets в Prometheus UI |
| Алерты не загружаются | Проверить синтаксис YAML, перезагрузить provisioning |

---

## 9. Принятые решения (фиксация)

- **Образ kafka-exporter:** `danielqsj/kafka-exporter:latest` — стандарт de-facto, подтверждён через Context7.
- **Порт exporter:** `9308` (стандартный).
- **Адрес Kafka:** `kafka:29092` (внутренняя Docker-сеть, PLAINTEXT listener).
- **Формат конфигурации Prometheus:** подтверждён через Context7 (`/prometheus/docs`).
- **Без SASL/TLS:** для локального демо-стенда аутентификация не требуется.
