# Урок 5. Мониторинг в Prometheus и Grafana

> Формат: **наблюдение с управляемым сбоем** — будешь смотреть метрики и алерты, а в конце
> ненадолго остановишь один сервис и вернёшь его обратно.
> Пререквизит: пройден урок 4 (ты запускал `etl_pipeline`, видел зелёный и красный DAG-run
> и понимаешь, где Airflow показывает судьбу одного прогона).
> Эталонные пути:
> [`configs/prometheus.yml`](../../../configs/prometheus.yml),
> [`configs/statsd_mapping.yml`](../../../configs/statsd_mapping.yml),
> [`configs/grafana/provisioning/`](../../../configs/grafana/provisioning/).
>
> Поток метрик одной строкой:
> `ClickHouse / Kafka / Airflow → exporters → Prometheus → Grafana dashboards → Grafana alerts`
>
> О чём урок простыми словами: Airflow показывает конкретный запуск пайплайна, а мониторинг
> показывает состояние всего стенда. Мы посмотрим, живы ли сервисы, есть ли лаг в Kafka,
> не застрял ли Airflow, и как Grafana подсвечивает поломку.

---

## 1. Зачем и где в проде

После урока 4 у нас есть управляемый DAG: если в DDS появляются сироты, Airflow красит задачу
и не выпускает DM дальше. Это важно, но это взгляд изнутри одного запуска.

В проде нужен ещё один слой: **мониторинг**. Он отвечает на другие вопросы:

- жив ли ClickHouse и отдаёт ли метрики;
- видит ли Kafka брокер, топики и consumer lag;
- работает ли scheduler Airflow;
- есть ли очередь задач;
- появились ли failed queries или failed tasks;
- что изменилось не в одном DAG-run, а во времени.

**Prometheus** — база временных рядов для метрик. Он регулярно ходит в endpoints сервисов и
забирает числа: память, счётчики запросов, lag, heartbeat scheduler-а. Такой регулярный опрос
называется **scrape** («сбор метрик»).

**Exporter** — маленький мост между сервисом и Prometheus. Если сервис не отдаёт метрики в
удобном для Prometheus виде, exporter переводит их. В нашем стенде:

- ClickHouse сам отдаёт `/metrics` на порту `9126`;
- Kafka читается через `kafka-exporter` на порту `9308`;
- Airflow отправляет StatsD-метрики в `statsd-exporter`, а тот отдаёт Prometheus endpoint на
  порту `9102`.

**Grafana** — витрина поверх метрик. В ней мы смотрим дашборды и алерты. Дашборд отвечает
«что сейчас происходит?», а алерт отвечает «какое условие уже достаточно плохое, чтобы привлечь
внимание?».

> **В проде иначе.** Метрики отправляют в общий мониторинг компании, алерты уходят в Slack,
> PagerDuty или другой on-call канал, а пороги подбирают по SLO и истории нагрузки. Но учебный
> паттерн тот же: сервис отдаёт метрики, Prometheus их собирает, Grafana помогает увидеть
> состояние и поломку.

---

## 2. Руки: открываем дашборды и targets

Подними стенд и прогони маленький срез, если он ещё не поднят:

```bash
make up
make ddl
LIMIT=50 make data
```

После этого запусти `etl_pipeline` в Airflow с конфигом:

```json
{"full_refresh": true}
```

Нам нужны не идеальные объёмы, а живой стенд, в котором есть Kafka-топики, строки в ClickHouse
и хотя бы один прогон Airflow.

### Проверяем Prometheus targets

Открой Prometheus: `http://localhost:9090`. В меню зайди в **Status → Targets**.

Ожидаем три job:

| Job | Target внутри Docker | Что это значит |
|-----|----------------------|----------------|
| `clickhouse` | `clickhouse:9126` | ClickHouse отдаёт встроенный Prometheus endpoint |
| `kafka` | `kafka-exporter:9308` | `kafka-exporter` подключился к Kafka и отдаёт метрики |
| `airflow` | `statsd-exporter:9102` | `statsd-exporter` отдаёт метрики Airflow в формате Prometheus |

У всех трёх состояние должно быть `UP`. Если один target `DOWN`, Grafana дальше будет показывать
`No data` или старые значения.

То же можно проверить из терминала:

```bash
curl -s http://localhost:9090/api/v1/targets | grep -o '"job":"[^"]*"'
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'
```

### Открываем Grafana

Открой Grafana: `http://localhost:3000` (логин `admin`, пароль `admin`).

Если Grafana просит сменить пароль после первого входа, для учебного стенда нажми **Skip**.
Так все инструкции в курсе останутся одинаковыми: локальный пользователь `admin`, пароль
`admin`. Если ты уже сменил пароль раньше, используй свой новый пароль.

После входа ты попадаешь на домашний экран Grafana. Минимальная навигация на этот урок:

- **Dashboards** — раздел с готовыми дашбордами. Обычно он доступен из левого меню или через
  поиск по слову `Dashboards`;
- **Alerting → Alert rules** — список правил алертов. Он понадобится в управляемой правке;
- строка поиска сверху помогает быстро найти `ClickHouse Overview`, `Kafka Overview` или
  `Airflow Overview`, если меню выглядит иначе.

В **Dashboards** должны быть три дашборда:

- `ClickHouse Overview`;
- `Kafka Overview`;
- `Airflow Overview`.

Открой каждый и смотри не на красоту графиков, а на смысл: какой слой стенда он показывает и
какой вопрос помогает задать.

### ClickHouse Overview

URL: `http://localhost:3000/d/clickhouse-overview/clickhouse-overview`

Главные панели:

- **System Health** — общий блок про ресурсное состояние;
- **CPU Usage**, **Memory Resident**, **Memory Code** — насколько ClickHouse нагружен и сколько
  памяти держит;
- **Query Performance** — блок про запросы;
- **Queries per Second**, **Active Queries**, **Failed Queries (total)**, **Total Queries** —
  есть ли запросная активность и ошибки;
- **Inserted Rows/sec** — идут ли вставки;
- **MergeTree Storage**, **Total Parts**, **Parts by State**, **Total Merges**,
  **Merges per Second** — что происходит с MergeTree-частями.

Для учебного стенда здесь обычно не будет большой нагрузки. Это нормально. Главное — увидеть,
что метрики не пустые и меняются после запросов или загрузки данных.

### Kafka Overview

URL: `http://localhost:3000/d/kafka-overview/kafka-overview`

Главные панели:

- **Cluster Health** — жив ли Kafka-брокер и видны ли топики;
- **Brokers Up**, **Topics**, **Total Partitions**, **Consumer Groups** — базовый снимок
  кластера;
- **Throughput** и **Messages In / sec by Topic** — идут ли новые сообщения в топики;
- **Consumer Lag by Group** — насколько consumer group отстаёт от конца топика;
- **Partitions** и **Partition Offsets (Current)** — текущие offset-ы по партициям.

**Consumer lag** — это разница между тем, что уже лежит в топике, и тем, что consumer group
успела прочитать. В нашем стенде lag обычно быстро возвращается к нулю: данных мало, ClickHouse
читает быстро. Если lag растёт и не снижается, downstream не успевает за Kafka.

### Airflow Overview

URL: `http://localhost:3000/d/airflow-overview/airflow-overview`

Главные панели:

- **Scheduler Health** — жив ли scheduler;
- **DAG Bag Size** — сколько DAG-ов Airflow видит;
- **Parse Time** — сколько времени занимает разбор DAG-файлов;
- **Scheduler Heartbeat Rate** — продолжает ли scheduler отправлять heartbeat;
- **Executor Status**, **Open Slots**, **Queued Tasks**, **Running Tasks**,
  **Executor Tasks Over Time** — хватает ли executor-у места для задач;
- **Task Performance**, **Task Duration (avg)**, **Task Failures vs Success Rate** — как
  ведут себя задачи во времени.

После урока 4 тебе знаком красный `etl_pipeline` в UI Airflow. На этом дашборде та же проблема
видна шире: не «какая task упала в одном run», а «есть ли failed tasks как метрика во времени».

---

## 3. Загляни внутрь

Открой [`configs/prometheus.yml`](../../../configs/prometheus.yml). Это короткая карта того,
откуда Prometheus забирает метрики.

### `scrape_configs`: кого опрашивает Prometheus

В файле три блока:

```yaml
scrape_configs:
  - job_name: "clickhouse"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["clickhouse:9126"]
```

`job_name` — имя источника метрик. Его ты видел в Prometheus Targets. `metrics_path` говорит,
куда ходить за метриками. `targets` использует внутренние имена Docker Compose, а не
`localhost`: Prometheus живёт внутри compose-сети и ходит к соседним контейнерам по их service
name.

Kafka и Airflow устроены так же, но с exporter-ами:

- `kafka` → `kafka-exporter:9308`;
- `airflow` → `statsd-exporter:9102`.

### Почему Airflow идёт через StatsD

Открой [`configs/statsd_mapping.yml`](../../../configs/statsd_mapping.yml). Airflow отправляет
метрики в StatsD-формате, например `airflow.scheduler_heartbeat`. Prometheus так напрямую не
читает, поэтому между ними стоит `statsd-exporter`.

Mapping переводит имена в Prometheus-стиль:

| StatsD-метрика Airflow | Prometheus-метрика | Где видна |
|------------------------|--------------------|-----------|
| `airflow.scheduler_heartbeat` | `airflow_scheduler_heartbeat_total` | `Scheduler Heartbeat Rate` |
| `airflow.executor.queued_tasks` | `airflow_executor_queued_tasks` | `Queued Tasks` |
| `airflow.ti.finish.*.*.failed` | `airflow_task_failures_total` | `Task Failures vs Success Rate` |
| `airflow.dag.*.*.duration` | `airflow_task_duration_seconds` | `Task Duration (avg)` |

Звёздочки в mapping — это части имени, которые становятся label-ами. Например, у task duration
появляются `dag_id` и `task_id`, чтобы в Grafana можно было отличить один DAG и одну task от
других.

### Grafana provisioning: дашборды и алерты как файлы

Открой папку [`configs/grafana/provisioning/`](../../../configs/grafana/provisioning/).

В ней три вида настройки:

- `datasources/prometheus.yml` — говорит Grafana, где находится Prometheus;
- `dashboards/*.json` — описывает панели дашбордов;
- `alerting/*-alert-rules.yml` — описывает правила алертов.

Это называется **provisioning**: Grafana получает дашборды и алерты из файлов при старте, а не
только через ручные клики в UI. Для стенда это удобно: поднял compose — получил одинаковую
Grafana.

### Какие алерты уже есть

Открой **Alerting → Alert rules** в Grafana. Там должны быть правила:

| Группа | Правило | Условие простыми словами |
|--------|---------|--------------------------|
| ClickHouse Alerts | `ClickHouse Failed Queries Rate` | появились failed queries |
| ClickHouse Alerts | `ClickHouse Memory Resident High` | ClickHouse занял больше 85% памяти |
| ClickHouse Alerts | `ClickHouse Parts Active High` | активных MergeTree-частей больше 500 |
| Kafka Alerts | `Kafka Broker Down` | Prometheus видит меньше одного Kafka-брокера |
| Kafka Alerts | `Kafka Consumer Lag High` | lag больше 10000 |
| Kafka Alerts | `Kafka No Messages Produced` | в топики долго почти не идут новые сообщения |
| Airflow Alerts | `Airflow Scheduler Down` | heartbeat scheduler-а почти исчез |
| Airflow Alerts | `Airflow Queue Backlog` | в очереди больше 50 задач |
| Airflow Alerts | `High Task Failure Rate` | растёт rate failed tasks |
| Airflow Alerts | `High DAG Parse Time` | DAG-файлы долго парсятся |

Не все эти правила обязаны стрелять в учебном стенде. Часть порогов специально похожа на
продовые: они показывают, как формулируется условие, но не создают шум на каждом маленьком
прогоне.

---

## 4. Управляемая правка: остановим scheduler и увидим алерт

Теперь сделаем маленькую поломку, которую легко вернуть назад: остановим только
`airflow-scheduler`. Web UI Airflow останется доступен, ClickHouse и Kafka не трогаем.

### Шаг 1. Убедись, что сейчас всё живо

Открой Grafana → **Airflow Overview** и найди панель **Scheduler Heartbeat Rate**.

Потом открой **Alerting → Alert rules** и найди правило `Airflow Scheduler Down`. Перед
экспериментом оно должно быть в состоянии `Normal` (не `Alerting`).

Можно проверить и командой:

```bash
docker compose ps airflow-scheduler
```

### Шаг 2. Останови scheduler

```bash
docker compose stop airflow-scheduler
```

Подожди 2-3 минуты. У правила `Airflow Scheduler Down` стоит окно `for: 2m`, поэтому алерт не
обязан покраснеть мгновенно. Это нормальное поведение: мониторинг защищается от коротких
миганий.

Что ожидаем:

- в **Airflow Overview** панель **Scheduler Heartbeat Rate** падает к нулю или перестаёт
  обновляться;
- в **Alerting → Alert rules** правило `Airflow Scheduler Down` переходит в `Alerting`;
- новые DAG-runs не должны нормально планироваться, потому что scheduler остановлен.

Это и есть отличие мониторинга от ручной проверки: тебе не нужно помнить команду для heartbeat.
Условие уже записано в alert rule.

### Верни как было

Сразу верни scheduler:

```bash
docker compose up -d airflow-scheduler
```

Подожди ещё 2-3 минуты и проверь:

- `docker compose ps airflow-scheduler` показывает running/up;
- **Scheduler Heartbeat Rate** снова растёт;
- `Airflow Scheduler Down` выходит из `Alerting`.

Если Grafana или Prometheus после экспериментов показывают `No data`, восстанови мониторинг
штатной командой:

```bash
make reload-monitoring
```

Если это не помогло и видишь `out of bounds` или залипшие старые значения:

```bash
make recover-monitoring
```

---

## 5. Проверь себя

| Действие | Где смотреть | Что ожидать |
|----------|--------------|-------------|
| открыть Prometheus targets | `http://localhost:9090` → Status → Targets | `clickhouse`, `kafka`, `airflow` в состоянии `UP` |
| открыть `ClickHouse Overview` | Grafana dashboards | панели `Queries per Second`, `Failed Queries (total)`, `Inserted Rows/sec` не пустые |
| открыть `Kafka Overview` | Grafana dashboards | видны `Brokers Up`, `Topics`, `Consumer Lag by Group` |
| открыть `Airflow Overview` | Grafana dashboards | видны `Scheduler Heartbeat Rate`, `Queued Tasks`, `Task Failures vs Success Rate` |
| остановить `airflow-scheduler` | Grafana alert rules | `Airflow Scheduler Down` переходит в `Alerting` после окна ожидания |
| вернуть `airflow-scheduler` | Grafana alert rules и `docker compose ps` | сервис снова running/up, алерт возвращается в норму |

---

## 6. Что должно получиться

После урока у тебя на руках — видимый результат:

- скрин Prometheus Targets, где `clickhouse`, `kafka` и `airflow` находятся в `UP`;
- скрин Grafana с правилом `Airflow Scheduler Down` в `Alerting` после остановки scheduler-а;
- короткое объяснение своими словами: почему Airflow UI и Grafana отвечают на разные вопросы.

Проверь себя на словах — примерно эти вопросы всплывут на еженедельном созвоне:

- зачем Prometheus нужен отдельно от Grafana;
- чем exporter отличается от самого сервиса;
- почему Airflow-метрики проходят через `statsd-exporter`;
- что такое scrape target;
- что показывает consumer lag;
- почему алерт `Airflow Scheduler Down` не краснеет в ту же секунду, когда ты остановил сервис;
- почему после эксперимента нужно явно вернуть `airflow-scheduler`.

Если ответ про Airflow UI и Grafana получается одним и тем же, вернись к началу урока. Airflow UI
удобен для разбора конкретного DAG-run. Grafana удобна для состояния системы во времени.

---

## Мост к следующему шагу

Теперь стенд закрывает полный учебный маршрут: Kafka принимает поток, ClickHouse раскладывает
слои, Airflow управляет порядком, а Prometheus и Grafana показывают состояние системы. Дальше
этот же стенд можно использовать не как разовый набор уроков, а как тренажёр: менять данные,
ломать отдельные места, смотреть, где появляется сигнал, и объяснять по метрикам, что произошло.
