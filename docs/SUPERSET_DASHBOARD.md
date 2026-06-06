# Дашборд Superset для E-commerce Analytics

Документация по настройке и использованию Superset дашборда для анализа кликстрима.

---

## Быстрый старт

### 1. Запуск инфраструктуры

```bash
# Запуск всех сервисов
make up

# Применение DDL в ClickHouse
make ddl

# Загрузка данных в Kafka
make data

# Запуск ETL-пайплайна (ODS → DDS → DM)
make transform
```

### 2. Инициализация Superset

```bash
# Автоматическая инициализация (создание подключения и датасетов)
make superset-init

# Создание дашборда с чартами; при необходимости обновляет metadata колонок датасетов
make superset-dashboard
```

### 3. Доступ к UI

Откройте в браузере: http://localhost:8088

**Логин:** `admin`  
**Пароль:** `admin`

---

## Структура дашборда

### Витрины данных (Datasets)

| Витрина | Таблица ClickHouse | Описание |
|---------|-------------------|------------|
| **Events Enriched** | `dm.v_events_enriched` | Полная обогащённая витрина событий |
| **Daily Traffic** | `dm.v_daily_traffic` | Агрегаты по дням |
| **UTM Effectiveness** | `dm.v_utm_effectiveness` | Эффективность маркетинговых каналов |
| **Top Pages** | `dm.v_top_pages_daily` | Популярность страниц |
| **Session Overview** | `dm.v_session_overview` | Анализ сессий |
| **DQ Summary** | `dm.dq_summary` | Метрики по слоям (строки, ошибки, сироты) |

### Чарты (Charts)

#### KPI-блок (верх дашборда)
- **📊 Total Events** — общее количество событий
- **👤 Unique Users** — уникальные пользователи
- **📈 Avg Events/Visit** — среднее количество событий на визит (`click_id`)
- **🎯 Conversion to /confirmation** — доля просмотров `/confirmation` от просмотров `/home`

KPI разложены в одну строку по 12-колоночной сетке Superset: четыре блока по 3 колонки.
`Unique Sessions` не вынесен отдельной KPI-плиткой, потому что в демо-данных
`user_domain_id` и `click_id` идут 1:1 и дают то же число, что `Unique Users`.

#### Динамика трафика
- **📅 Events by Hour** — линейный график событий по часам
- **📱 Traffic by Device** — pie chart распределения по устройствам

#### География
- **🌍 Geography Map** — world map с распределением по странам

#### Маркетинг
- **🔗 UTM Effectiveness Table** — таблица эффективности UTM-меток
- **🪜 Page Funnel** — funnel chart по просмотрам страниц, от `/home` к `/confirmation`

> **Что проверили по Superset 4.1.2.** Через MCP Context7 проверили официальную
> библиотеку `/apache/superset`; документация не дала точной строки `viz_type`.
> В установленном Superset 4.1.2 дополнительно проверили bundled example
> `Featured Charts/Funnel.yaml` и frontend assets: для воронки используется
> `viz_type: funnel`, поэтому dashboard создаёт именно funnel chart.

#### Прохождение строк по слоям
- **🧱 Rows by Layer (event)** — `dist_bar` по `dm.dq_summary`: сколько строк
  одного **event-зерна** в каждом слое конвейера `STG → ODS → DDS → DM`.

> **Почему именно одно зерно, а не сумма по слою.** Чарт берёт по одной
> канонической таблице на слой (`browser_raw → browser_event → event →
> v_events_enriched`). Если суммировать `total_rows` по всем таблицам слоя,
> в один столбец складываются таблицы разного зерна (события `1000` + визиты `99`
> + пустые error-таблицы) и получается **ложная «воронка потерь»**, которой нет.
> На одном зерне убывание становится настоящим: видимый шаг **1050 → 1000** —
> это дедупликация at-least-once потока по `event_id` в ODS
> (`ReplacingMergeTree`), а дальше число стабильно до витрины.
>
> Настоящие сигналы качества (`rows_with_errors` в ODS, `orphan_events` в DDS)
> на чистых демо-данных равны нулю и живут в `dm.dq_summary` отдельными
> `check_name` — их разбирают уроки 3–4, а не этот чарт.

> **Порядок столбцов.** В groupby подпись слоя получает числовой префикс
> (`1 · stg`, `2 · ods`, …), а `order_bars` сортирует бары по подписи — иначе
> `dist_bar` ставит их по убыванию значения, а не по порядку конвейера.

### Фильтры (Native Filters)

| Фильтр | Поле | Тип | Применение |
|--------|------|-----|------------|
| 📅 Date Range | `event_date` | Time Range | Все чарты; по умолчанию `No filter`, чтобы демо-данные 2022 года не скрывались |
| 🌍 Country | `geo_country` | Multi-select | Все чарты |
| 📱 Device Type | `device_type` | Multi-select | Все чарты |
| 🌐 Browser | `browser_name` | Multi-select | Все чарты |

---

## Команды Makefile

```bash
# Основные
make up          # Запуск всех сервисов
make down        # Остановка сервисов
make clean      # Остановка с удалением volumes
make logs service=superset  # Логи сервиса

# ETL
make ddl         # Применение DDL в ClickHouse
make data        # Загрузка данных в Kafka
make transform   # Запуск batch-процесса

# Superset
make superset-init      # Инициализация (подключение + датасеты)
make superset-dashboard # Создание дашборда
make superset-export    # Экспорт дашборда в JSON
make superset-ui        # Показать URL и логин
make superset-restart   # Перезапуск сервиса
```

---

## Ручная настройка (если автоматика не сработала)

### Создание подключения к ClickHouse

1. Откройте **Settings → Database Connections**
2. Нажмите **+ Database**
3. Выберите **ClickHouse**
4. Введите SQLAlchemy URI:
   ```
   clickhousedb://default:123456@clickhouse:8123/default
   ```
5. Установите:
   - **Expose in SQL Lab:** ✅
   - **Allow DDL:** ❌
6. Нажмите **Connect**

### Импорт датасетов

```bash
# Внутри контейнера
docker compose exec superset bash
python /app/superset_init/init_superset.py
```

### Создание чартов вручную

1. Перейдите в **Charts → + Chart**
2. Выберите датасет (например, `dm.v_events_enriched`)
3. Настройте визуализацию:
   - **Viz Type:** Big Number / Line Chart / Pie Chart / World Map / Table
   - **Metrics:** COUNT(*), COUNT(DISTINCT ...)
   - **Dimensions:** группировки
   - **Filters:** фильтры
4. Нажмите **Create Chart**

### Создание дашборда

1. **Dashboards → + Dashboard**
2. Назовите: "E-commerce Analytics Dashboard"
3. Добавьте чарты из списка
4. Настройте layout (drag-and-drop)
5. Добавьте Native Filters (фильтры вверху)
6. Сохраните

---

## Экспорт и импорт дашборда

### Экспорт

```bash
# Автоматический экспорт в JSON
make superset-export

# Результат: superset/dashboards/ecommerce_analytics.json
```

### Импорт

```bash
# Импорт через CLI
docker compose exec superset superset import-dashboards -p /app/superset_init/dashboards/ecommerce_analytics.json

# Или через UI: Settings → Import Dashboards
```

---

## Расширение дашборда

### Добавление нового чарта

1. Отредактируйте `superset/create_dashboard.py`
2. Добавьте конфигурацию в `CHARTS_CONFIG`
3. Запустите: `make superset-dashboard`

Пример нового чарта:
```python
{
    "slice_name": "📊 My New Chart",
    "viz_type": "echarts_bar",
    "dataset_name": "v_events_enriched",
    "params": {
        "x_axis": "event_type",
        "metrics": [{"sqlExpression": "COUNT(*)", "label": "Count"}],
        "time_range": "No filter"
    }
}
```

---

## Troubleshooting

### Superset не стартует

```bash
# Проверить логи
make logs service=superset

# Перезапуск
make superset-restart

# Полная переинициализация
docker compose down -v
docker compose up -d
make ddl
make data
make transform
make superset-init
make superset-dashboard
```

### Нет данных в чартах

```bash
# Проверить данные в ClickHouse
docker compose exec clickhouse clickhouse-client -q "SELECT count() FROM dm.v_events_enriched"

# Перезапустить ETL
make transform
```

### Ошибка подключения к ClickHouse

```bash
# Проверить доступность ClickHouse
docker compose exec superset bash -c "ping clickhouse"

# Проверить порт
docker compose exec superset bash -c "curl clickhouse:8123"
```

---

## Порты сервисов

| Сервис | URL | Логин/Пароль |
|--------|-----|--------------|
| Superset | http://localhost:8088 | admin / admin |
| ClickHouse HTTP | http://localhost:9123 | default / (пустой) |
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Kafka UI | http://localhost:8082 | - |

---

## Дополнительные ресурсы

- [Superset Documentation](https://superset.apache.org/docs/intro)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference)
- [ARCHITECTURE.md](./ARCHITECTURE.md) — архитектура хранилища
