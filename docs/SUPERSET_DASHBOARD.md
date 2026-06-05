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
| **DQ Summary** | `dm.dq_summary` | Качество данных |

### Чарты (Charts)

#### KPI-блок (верх дашборда)
- **📊 Total Events** — общее количество событий
- **👤 Unique Users** — уникальные пользователи
- **🎯 Unique Sessions** — уникальные сессии (click_id)
- **📈 Avg Events/Session** — среднее количество событий на сессию

KPI разложены в одну строку по 12-колоночной сетке Superset: четыре блока по 3 колонки.

#### Динамика трафика
- **📅 Events by Hour** — линейный график событий по часам
- **📱 Traffic by Device** — pie chart распределения по устройствам

#### География
- **🌍 Geography Map** — world map с распределением по странам

#### Маркетинг
- **🔗 UTM Effectiveness Table** — таблица эффективности UTM-меток
- **📄 Top Pages** — bar chart топ-20 страниц

#### Качество данных
- **🔍 Data Quality Summary** — статистика по слоям STG/ODS/DDS

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
make superset-init
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
