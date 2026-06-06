# Урок 6. BI-витрина в Superset

> Формат: **практика** — будешь запускать Superset поверх готовых витрин ClickHouse,
> смотреть дашборд и делать маленькую обратимую правку в конфигурации чарта.
> Пререквизит: пройдены уроки 4–5 (ты уже запускал `etl_pipeline`, видел DM-слой в конце
> пайплайна и понимаешь разницу между мониторингом стенда и данными для анализа).
> Эталонные пути:
> [`sql/ddl/dm/40_dm.sql`](../../../sql/ddl/dm/40_dm.sql),
> [`sql/dm/40_dds_to_dm.sql`](../../../sql/dm/40_dds_to_dm.sql),
> [`superset/init_superset.py`](../../../superset/init_superset.py),
> [`superset/create_dashboard.py`](../../../superset/create_dashboard.py).
>
> Поток данных одной строкой:
> `DDS → DM views / dm.dq_summary → Superset datasets → charts → dashboard → native filters`
>
> О чём урок простыми словами: ClickHouse уже подготовил таблицы и представления для
> потребления. Superset превращает их в экран для аналитика: датасеты, графики, фильтры
> и один общий дашборд.

---

## 1. Зачем и где в проде

После уроков 1–4 у нас есть данные: поток приземлился в STG, разобрался в ODS, собрался в DDS,
а в конце появился DM-слой. После урока 5 у нас есть мониторинг: он отвечает, жив ли стенд и
не сломался ли пайплайн.

Теперь нужен другой взгляд — **BI** (Business Intelligence, «аналитический интерфейс для
бизнес-вопросов»). BI отвечает не «жив ли ClickHouse», а:

- сколько событий пришло;
- какие устройства чаще встречаются;
- из каких стран пришёл трафик;
- какие UTM-каналы дают больше кликов;
- какие страницы самые популярные;
- есть ли видимые проблемы качества данных.

**Superset** — BI-инструмент. Он не заменяет ClickHouse, Airflow или Grafana. Он сидит поверх
готовых данных и даёт интерфейс для просмотра, фильтрации и сборки графиков.

В нашем стенде роли такие:

| Слой | Что делает |
|------|------------|
| `dds.click`, `dds.event` | хранит собранные сущности после ODS |
| `dm.v_*`, `dm.dq_summary` | готовит поверхность потребления для аналитики |
| Superset dataset | регистрирует таблицу или VIEW из ClickHouse в Superset |
| Superset chart | сохраняет один график или KPI на базе dataset |
| Superset dashboard | собирает charts в один экран |
| Native filters | фильтруют dashboard по дате, стране, устройству, браузеру |

Граница урока: **витрина DM и BI-экран — не одно и то же**.

DM-витрина — это SQL-объект в ClickHouse. Она задаёт форму данных: какие поля есть, на какой
гранулярности лежит агрегат, какие joins уже сделаны. BI-экран — это способ показать эту
витрину человеку: график, таблица, фильтр, порядок блоков на странице.

> **В проде иначе.** Superset обычно подключают к нескольким хранилищам, заводят роли и права,
> разделяют черновые и опубликованные дашборды, а тяжёлые витрины материализуют. Но базовая
> схема та же: хранилище готовит данные, BI даёт удобную точку потребления.

---

## 2. Руки: запускаем Superset и смотрим дашборд

Подними стенд и прогони полный демо-датасет:

```bash
make up
make ddl
make data
make transform
```

`make transform` прогоняет цепочку STG → ODS → DDS → DM вне Airflow. Для этого урока так
быстрее: нам нужен готовый DM-слой, а не разбор DAG. Отладочный срез через
`LIMIT=50 make data` можно использовать для быстрых экспериментов, но эталонный dashboard
и числа урока рассчитаны на полном наборе данных.

Теперь инициализируй Superset:

```bash
make superset-init
```

Эта команда создаёт или обновляет:

- подключение `clickhouse_dwh`;
- 6 datasets поверх `dm.*`.

После этого создай или обнови charts и сам dashboard:

```bash
make superset-dashboard
```

Эта команда создаёт или обновляет:

- 10 charts;
- dashboard `E-commerce Analytics Dashboard`.

Открой Superset: `http://localhost:8088` (логин `admin`, пароль `admin`).

Если Superset предлагает сменить пароль после первого входа, для учебного стенда можно нажать
**Skip**. В проде так не делают, но локальный курс держит одинаковые инструкции для всех.

### Открываем dashboard

Открой готовый dashboard:

```text
http://localhost:8088/superset/dashboard/1/
```

Если URL не открылся, зайди через меню **Dashboards** и найди `E-commerce Analytics Dashboard`.

На экране должны быть блоки:

- KPI сверху: `Total Events`, `Unique Users`, `Avg Events/Visit`,
  `Conversion to /confirmation`;
- динамика: `Events by Hour`, `Traffic by Device`;
- география: `Geography Map`;
- маркетинг: `UTM Effectiveness Table`, `Page Funnel`;
- прохождение строк по слоям: `Rows by Layer (event)`.

`Conversion to /confirmation` считается как просмотры `/confirmation` / просмотры `/home`.
Это page-funnel метрика, а не доля визитов: она совпадает с тем, как ниже устроен chart
`Page Funnel`.

`Rows by Layer (event)` показывает, сколько строк доходит до каждого слоя конвейера
`STG → ODS → DDS → DM`. Прежде чем читать чарт, договоримся про одно слово.

> **Зерно (grain), он же уровень гранулярности — это что считается одной строкой
> таблицы.** У события зерно
> «одно событие = одна строка» (ключ `event_id`), у визита — «один визит = одна
> строка» (ключ `click_id`). Это разные зёрна: событий 1000, а визитов 99, потому
> что в одном визите много событий. Складывать строки разного зерна в одно число
> бессмысленно — это всё равно что сложить «штуки яблок» и «корзины яблок».

Поэтому чарт держит **одно зерно — event**: берёт по одной канонической таблице
событий на слой (`browser_raw → browser_event → event → v_events_enriched`), а не сумму
по слою. Если просуммировать все таблицы слоя, в один столбец попадут таблицы разного
зерна (события 1000 + визиты 99 + пустые error-таблицы) и получится «воронка потерь»,
которой на самом деле нет.

Шаг **1050 → 1000** на первом переходе — это не потеря данных, а дедупликация
at-least-once потока по `event_id` в ODS (`ReplacingMergeTree`): в STG приехало 1050 строк,
но уникальных `event_id` среди них — 1000 (часть событий Kafka доставила повторно). Дальше
число стабильно. Настоящие проблемы качества (ошибки парсинга, осиротевшие события) на
чистых демо-данных равны нулю и лежат в `dm.dq_summary` отдельными `check_name` — их
разбирали уроки 3–4.

### Фильтр даты

В демо-данных события датированы `2022-11-28`. В текущей конфигурации dashboard фильтр даты
открывается как `No filter`. Если у тебя осталась старая metadata Superset и native filter
**Date Range** стоит в значении `Last week`, часть графиков может быть пустой, хотя данные есть.

Для этого урока поставь в фильтре даты одно из двух:

- `No filter`;
- или ручной диапазон вокруг `2022-11-28`.

После этого нажми **Apply filters**. Теперь смотри на dashboard как аналитик: какие графики
отвечают на бизнес-вопросы, а какие только показывают техническое устройство конвейера.

### Проверяем данные напрямую в ClickHouse

Открой ClickHouse play-консоль: `http://localhost:9123/play`.

Проверь, что основной dataset Superset не пустой:

```sql
SELECT count() AS events
FROM dm.v_events_enriched;
```

И посмотри, откуда берётся график `Page Funnel`:

```sql
SELECT page_url_path, sum(pageviews) AS pageviews
FROM dm.v_top_pages_daily
GROUP BY page_url_path
ORDER BY pageviews DESC
LIMIT 20;
```

Запомни эту связку: Superset показывает график, но данные и логика агрегации живут в ClickHouse.

---

## 3. Загляни внутрь

Разберём три места: DM-витрины в ClickHouse, регистрацию datasets в Superset и сборку charts /
dashboard.

### DM: поверхность потребления

Открой [`sql/ddl/dm/40_dm.sql`](../../../sql/ddl/dm/40_dm.sql).

В начале файла написано, почему DM сейчас сделан через `VIEW`:

- логику можно менять без пересоздания таблиц;
- нет копии данных поверх DDS;
- для демо производительности достаточно.

Основная витрина для dashboard — `dm.v_events_enriched`. Она соединяет `dds.event` и `dds.click`
через `click_id`:

```sql
FROM dds.event AS e
LEFT JOIN dds.click AS c ON c.click_id = e.click_id;
```

`LEFT JOIN` здесь осознанный: событие может существовать без части контекста из клика. Для BI это
значит: график событий не исчезает только потому, что у части строк нет устройства или географии.

Другие VIEW дают более узкие поверхности:

| VIEW / таблица | Для чего нужна в Superset |
|----------------|---------------------------|
| `dm.v_events_enriched` | KPI, динамика, устройства, география, фильтры |
| `dm.v_daily_traffic` | готовая дневная агрегация трафика |
| `dm.v_utm_effectiveness` | таблица по UTM-каналам |
| `dm.v_top_pages_daily` | популярные страницы |
| `dm.v_session_overview` | обзор сессий |
| `dm.dq_summary` | метрики по слоям: строки (`total_rows`), ошибки, сироты |

Открой [`sql/dm/40_dds_to_dm.sql`](../../../sql/dm/40_dds_to_dm.sql). Этот файл не пересчитывает
все `dm.v_*`: VIEW создаются в DDL. Здесь batch-часть наполняет `dm.dq_summary` метриками по
слоям (включая строку для слоя `dm`), чтобы dashboard мог показать прохождение строк по
конвейеру после каждого прогона.

### `init_superset.py`: подключение и datasets

Открой [`superset/init_superset.py`](../../../superset/init_superset.py).

Сначала скрипт собирает URI ClickHouse:

```python
return f"clickhousedb://{user}:{password}@{host}:{port}/{database}"
```

Внутри Docker-сети Superset ходит в ClickHouse по HTTP-порту `8123`, поэтому URI использует
`clickhousedb://...@clickhouse:8123/default`.

Потом скрипт создаёт подключение `clickhouse_dwh` и регистрирует datasets:

```python
datasets = [
    {
        "table_name": "v_events_enriched",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Полная обогащённая витрина событий (event + click)"
    },
    ...
]
```

**Dataset** в Superset — это не копия данных. Это запись в metadata Superset: какая таблица или
VIEW есть в ClickHouse, какие у неё колонки и как её можно использовать в графиках.

Metadata Superset живёт в PostgreSQL, а сами данные остаются в ClickHouse. Поэтому после полного
сброса volumes нужно заново создать metadata Superset, а после пересчёта данных — сами charts
обычно остаются теми же.

### `create_dashboard.py`: charts, dashboard, filters

Открой [`superset/create_dashboard.py`](../../../superset/create_dashboard.py).

В `CHARTS_CONFIG` лежит список charts. Один элемент списка — один график:

```python
{
    "slice_name": "🪜 Page Funnel",
    "viz_type": "funnel",
    "dataset_name": "v_top_pages_daily",
    "params": {
        "groupby": ["page_url_path"],
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "SUM(pageviews)",
            "label": "Pageviews"
        },
        "row_limit": 20,
        "time_range": "No filter",
        "sort_by_metric": True,
        "percent_calculation_type": "first_step"
    }
}
```

Здесь видно четыре идеи:

- `slice_name` — имя chart в Superset;
- `viz_type` — тип визуализации;
- `dataset_name` — на каком dataset строится chart;
- `params` — настройка запроса и отображения.

Ниже `DASHBOARD_CONFIG` задаёт сам dashboard:

```python
DASHBOARD_CONFIG = {
    "dashboard_title": "🛒 E-commerce Analytics Dashboard",
    "description": "...",
    "published": True,
    "slug": "ecommerce-analytics",
}
```

Dashboard находится по `slug`, а charts добавляются в layout. Если dashboard уже существует,
скрипт обновляет metadata, layout и список charts. На этом держится управляемая правка: можно
поменять параметр chart, запустить `make superset-dashboard` и увидеть результат в UI.

Native filters создаются в `build_dashboard_metadata`. Там есть фильтры:

- `Date Range` по `event_date`;
- `Country` по `geo_country`;
- `Device Type` по `device_type`;
- `Browser` по `browser_name`.

> **Что проверили по API.** Перед уроком Superset сверили через MCP Context7 (`/apache/superset`):
> в Superset есть отдельные сущности charts и dashboards, metadata хранится отдельно от
> подключаемых источников данных. Для `funnel` Context7 не дал точную строку `viz_type`, поэтому
> дополнительно проверили установленный Superset 4.1.2: bundled example
> `Featured Charts/Funnel.yaml` использует `viz_type: funnel`. Поэтому в уроке не лезем в REST API
> Superset, а работаем через уже существующий скрипт стенда.

---

## 4. Управляемая правка: уменьшаем Page Funnel

Сейчас chart `Page Funnel` показывает до 20 страниц:

```python
"row_limit": 20,
```

Сделай маленькую видимую правку: временно покажи только топ-3 страницы.

Открой [`superset/create_dashboard.py`](../../../superset/create_dashboard.py), найди chart
`Page Funnel` и поменяй:

```python
"row_limit": 20,
```

на:

```python
"row_limit": 3,
```

Запусти обновление dashboard:

```bash
make superset-dashboard
```

Вернись в Superset и обнови страницу dashboard. В chart `Page Funnel` должно остаться не больше
трёх страниц. Если фильтр даты снова скрыл данные, поставь **Date Range → No filter** и нажми
**Apply filters**.

Почему это хорошая маленькая правка:

- мы не меняем SQL-витрину в ClickHouse;
- не создаём новый dataset;
- не трогаем подключение к ClickHouse;
- меняем только BI-представление уже готовых данных.

### Верни как было

Верни в [`superset/create_dashboard.py`](../../../superset/create_dashboard.py):

```python
"row_limit": 20,
```

И снова запусти:

```bash
make superset-dashboard
```

После обновления страницы chart `Page Funnel` снова может показывать до 20 страниц.

Если после экспериментов Superset выглядит странно, самый простой учебный возврат dashboard
metadata к конфигурации из репозитория:

```bash
make superset-dashboard
```

Данные в ClickHouse эта команда не удаляет. Она повторно применяет charts и dashboard
metadata Superset.

---

## 5. Проверь себя

| Действие | Где смотреть | Что ожидать |
|----------|--------------|-------------|
| `make transform` | ClickHouse `dm.v_events_enriched` | `count() > 0` |
| `make superset-init` | Superset → **Settings → Database Connections** | есть подключение `clickhouse_dwh` |
| открыть **Datasets** | Superset UI | есть datasets `v_events_enriched`, `v_top_pages_daily`, `dq_summary` |
| открыть dashboard | Superset UI | видны KPI, маркетинг, география и прохождение строк по слоям |
| поставить **Date Range → No filter** | dashboard filters | графики не скрываются из-за даты `2022-11-28` |
| поменять `row_limit` у `Page Funnel` на `3` и запустить `make superset-dashboard` | chart `Page Funnel` | не больше трёх страниц |
| вернуть `row_limit` на `20` и запустить `make superset-dashboard` | chart `Page Funnel` | ограничение снова до 20 страниц |

Вопросы для созвона:

- чем DM-витрина отличается от Superset dataset;
- почему Superset не должен ходить напрямую в сырые STG-таблицы;
- зачем dashboard нужен `Date Range`, если SQL-витрина уже агрегирована;
- почему изменение `row_limit` — это BI-правка, а не изменение модели данных;
- где хранятся данные, а где metadata Superset.

---

## 6. Что должно получиться

К концу урока у тебя должен быть открытый dashboard `E-commerce Analytics Dashboard` в Superset.
Сделай скриншот после временной правки `Page Funnel`: на нём должно быть видно, что chart показывает
не больше трёх страниц.

Второй артефакт — короткий абзац своими словами:

> DM-слой в ClickHouse готовит данные для потребления, dataset в Superset регистрирует эту
> витрину, chart задаёт один график, dashboard собирает charts в экран, а native filters дают
> аналитику быстрый способ менять срез данных.

После этого обязательно верни `row_limit` на `20`, чтобы следующий урок или следующий прогон
стенда начинался с исходной конфигурации.

---

## Мост после курса

Теперь у тебя есть сквозная цепочка: Kafka → ClickHouse STG → ODS → DDS → DM → мониторинг →
Superset. Следующий честный вопрос уже не про этот стенд, а про продакшен: какие витрины стоит
материализовать, какие права дать BI-пользователям и как не превратить dashboard в единственный
источник правды вместо версионированного SQL в репозитории.
