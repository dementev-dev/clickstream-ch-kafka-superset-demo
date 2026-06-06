#!/usr/bin/env python3
"""
================================================================================
Скрипт создания дашборда "E-commerce Analytics" в Superset
================================================================================
Назначение:
    - Создание чартов (Charts) на основе датасетов DM-слоя
    - Создание дашборда с layout и native filters

Запуск:
    Внутри контейнера superset:
    python /app/superset_init/create_dashboard.py
================================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, '/app')


OBSOLETE_CHART_NAMES = {
    "🎯 Unique Sessions",
}


# Конфигурация чартов
CHARTS_CONFIG = [
    # KPI блок
    {
        "slice_name": "📊 Total Events",
        # big_number_total = итог по всему срезу (без granularity), а не последний
        # временной бакет, как у big_number.
        "viz_type": "big_number_total",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "column": None,
                "aggregate": None,
                "label": "Total Events",
                "optionName": "metric_1"
            },
            "y_axis_format": ",d",
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "👤 Unique Users",
        "viz_type": "big_number_total",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(DISTINCT user_domain_id)",
                "label": "Unique Users",
                "optionName": "metric_2"
            },
            "y_axis_format": ",d",
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "📈 Avg Events/Visit",
        "previous_slice_names": ["📈 Avg Events/Session"],
        "viz_type": "big_number_total",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*) / COUNT(DISTINCT click_id)",
                "label": "Avg Events/Visit",
                "optionName": "metric_4"
            },
            "y_axis_format": ".1f",
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "🎯 Conversion to /confirmation",
        "viz_type": "big_number_total",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": (
                    "if(countIf(page_url_path = '/home') = 0, 0, "
                    "countIf(page_url_path = '/confirmation') / countIf(page_url_path = '/home'))"
                ),
                "label": "Conversion to /confirmation",
                "optionName": "metric_5"
            },
            "y_axis_format": ".1%",
            "time_range": "No filter"
        }
    },
    # Динамика
    {
        "slice_name": "📅 Events over Time",
        "previous_slice_names": ["📅 Events by Hour"],
        # Все события стенда укладываются в ~50 минут (20:51–21:41 28.11.2022),
        # поэтому часовая гранулярность давала всего 2 точки и прямую линию,
        # читавшуюся как ошибка. 5-минутные бакеты дают ~10 точек — реальную
        # форму трафика. Поэтому и название не «by Hour», а «over Time».
        "viz_type": "echarts_timeseries_line",
        "dataset_name": "v_events_enriched",
        "params": {
            "granularity_sqla": "event_ts",
            "time_grain_sqla": "PT5M",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Events"
                }
            ],
            "groupby": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "row_limit": 10000
        }
    },
    {
        "slice_name": "📱 Traffic by Device",
        "viz_type": "pie",
        "dataset_name": "v_events_enriched",
        "params": {
            "groupby": ["device_type"],
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Count"
            },
            "row_limit": 100,
            "donut": True,
            "show_legend": True,
            "labels_outside": True,
            "time_range": "No filter"
        }
    },
    # География
    {
        "slice_name": "🌍 Geography Map",
        "viz_type": "world_map",
        "dataset_name": "v_events_enriched",
        "params": {
            "entity": "geo_country",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Events"
            },
            "row_limit": 500,
            "linear_color_scheme": "blue_white_yellow",
            "time_range": "No filter"
        }
    },
    # Маркетинг
    {
        "slice_name": "🔗 UTM Effectiveness Table",
        "viz_type": "table",
        "dataset_name": "v_utm_effectiveness",
        "params": {
            "groupby": ["utm_source", "utm_medium", "utm_campaign"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(clicks)", "label": "Clicks"},
                {"expressionType": "SQL", "sqlExpression": "SUM(uniq_users)", "label": "Users"},
                {"expressionType": "SQL", "sqlExpression": "SUM(uniq_sessions)", "label": "Sessions"}
            ],
            "row_limit": 100,
            "time_range": "No filter",
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "expressionType": "SQL",
                    "sqlExpression": "utm_source IS NOT NULL",
                    "subject": None,
                    "operator": None,
                    "comparator": None
                }
            ]
        }
    },
    {
        "slice_name": "🪜 Page Funnel",
        "previous_slice_names": ["📄 Top Pages"],
        # В Superset 4.1.2 `funnel` есть в frontend-плагинах и примерах
        # поставки, хотя legacy registry `superset.viz.viz_types` его не
        # показывает. Параметры взяты из bundled Featured Charts/Funnel.yaml.
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
            "percent_calculation_type": "first_step",
            "color_scheme": "supersetColors",
            "show_legend": True,
            "legendOrientation": "top",
            "legendMargin": 50,
            "tooltip_label_type": 5,
            "number_format": "SMART_NUMBER",
            "show_labels": True,
            "show_tooltip_labels": True
        }
    },
    # Прохождение строк по слоям (lineage одного event-зерна)
    {
        "slice_name": "🧱 Rows by Layer (event)",
        "previous_slice_names": ["🔍 Data Quality Summary"],
        # Честный row-lineage одного event-зерна через слои stg→ods→dds→dm.
        # Берём ПО ОДНОЙ канонической таблице на слой (browser_raw → browser_event
        # → event → v_events_enriched). Прежний вариант суммировал total_rows по
        # ВСЕМ таблицам слоя — таблицы разного зерна (события 1000 + визиты 99 +
        # error-таблицы 0) складывались в один столбец и рисовали ложную «воронку
        # потерь», которой нет. На одном зерне убывание становится настоящим:
        # видимый шаг 1050→1000 — это дедупликация at-least-once потока по
        # event_id в ODS (ReplacingMergeTree), а не потеря данных.
        # Префикс "N · " в groupby задаёт порядок слоёв (order_bars сортирует по
        # подписи), иначе бары встают по убыванию значения, а не по конвейеру.
        "viz_type": "dist_bar",
        "dataset_name": "dq_summary",
        "params": {
            "groupby": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": (
                        "multiIf(layer = 'stg', '1 · stg', layer = 'ods', '2 · ods', "
                        "layer = 'dds', '3 · dds', '4 · dm')"
                    ),
                    "label": "Layer"
                }
            ],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(check_value)", "label": "Rows"}
            ],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "expressionType": "SQL",
                    "sqlExpression": (
                        "check_name = 'total_rows' AND table_name IN "
                        "('browser_raw', 'browser_event', 'event', 'v_events_enriched')"
                    ),
                    "subject": None,
                    "operator": None,
                    "comparator": None
                }
            ],
            "order_bars": True,
            "row_limit": 100,
            "time_range": "No filter",
            "y_axis_format": ",d",
            "show_legend": False
        }
    }
]

# Конфигурация дашборда
DASHBOARD_CONFIG = {
    "dashboard_title": "🛒 E-commerce Analytics Dashboard",
    "description": "Аналитический дашборд для e-commerce кликстрима: трафик, конверсии, география и прохождение строк по слоям.",
    "published": True,
    "slug": "ecommerce-analytics",
}


# Раскладка дашборда по строкам (Superset layout v2 — флекс-модель ROW/CHART).
# Superset НЕ использует абсолютные координаты x/y: ширина чарта (1..12) имеет
# смысл только внутри строки-контейнера ROW. Без ROW каждый чарт занимает всю
# ширину и валится в одну колонку. Поэтому раскладываем именно строками.
# Каждая строка — список (slice_name, width); сумма width в строке должна быть ≤ 12.
DASHBOARD_ROWS = [
    # KPI-полоса: 4 числа в один ряд
    [("📊 Total Events", 3), ("👤 Unique Users", 3),
     ("📈 Avg Events/Visit", 3), ("🎯 Conversion to /confirmation", 3)],
    # Динамика во времени + разрез по устройствам
    [("📅 Events over Time", 8), ("📱 Traffic by Device", 4)],
    # География + эффективность маркетинговых каналов
    [("🌍 Geography Map", 6), ("🔗 UTM Effectiveness Table", 6)],
    # Популярные страницы + прохождение строк по слоям
    [("🪜 Page Funnel", 6), ("🧱 Rows by Layer (event)", 6)],
]

# Высота строки в grid-units Superset (одинаковая для всех чартов строки —
# чтобы они выравнивались по нижней границе).
ROW_HEIGHTS = [30, 60, 60, 60]


def sync_query_context(chart, params: dict, dataset_id: int) -> None:
    """
    Синхронизирует сохраненный query_context с обновленными params чарта.

    Для Superset 4.x у `big_number` запрос валидируется как time-series и
    ожидает `granularity` в query_context (проверено по актуальной документации).
    """
    if not chart.query_context:
        return

    try:
        query_context = json.loads(chart.query_context)
    except (TypeError, json.JSONDecodeError):
        logger.warning("Chart ID %s has invalid query_context, skip sync", chart.id)
        return

    query_context["datasource"] = {"id": dataset_id, "type": "table"}
    query_context["form_data"] = {
        **params,
        "datasource": f"{dataset_id}__table",
        "viz_type": chart.viz_type,
        "slice_id": chart.id,
    }

    queries = query_context.get("queries")
    if not isinstance(queries, list) or not queries:
        chart.query_context = json.dumps(query_context)
        return

    if chart.viz_type == "big_number_total":
        # Итоговое число по всему срезу: не time-series, без granularity и
        # time-grain — иначе запрос группируется по времени и число «прыгает»
        # на значение последнего бакета.
        query = queries[0]
        query["is_timeseries"] = False
        query.pop("granularity", None)
        query["time_range"] = params.get("time_range", query.get("time_range"))
        if "metric" in params:
            query["metrics"] = [params["metric"]]
        extras = query.get("extras") if isinstance(query.get("extras"), dict) else {}
        extras.pop("time_grain_sqla", None)
        query["extras"] = extras
    elif params.get("x_axis") or params.get("groupby"):
        # Для категориальных графиков синхронизируем колонки измерений.
        dimensions = params.get("groupby")
        if not dimensions and params.get("x_axis"):
            dimensions = [params["x_axis"]]
        query = queries[0]
        query["columns"] = dimensions
        if "metrics" in params:
            query["metrics"] = params["metrics"]
        elif "metric" in params:
            query["metrics"] = [params["metric"]]
        query["row_limit"] = params.get("row_limit", query.get("row_limit"))
        query["time_range"] = params.get("time_range", query.get("time_range"))
        query["is_timeseries"] = False

    chart.query_context = json.dumps(query_context)


def build_dashboard_metadata(filter_dataset_id: int | None) -> str:
    """Формирует json_metadata с валидными datasetId для native filters."""
    native_filters = []

    if filter_dataset_id is not None:
        native_filters = [
            {
                "id": "date_filter",
                "name": "📅 Date Range",
                "filterType": "filter_time",
                "targets": [{"datasetId": filter_dataset_id, "column": {"name": "event_date"}}],
                "defaultValue": "No filter",
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "cascadeParentIds": [],
                "isInstant": True
            },
            {
                "id": "country_filter",
                "name": "🌍 Country",
                "filterType": "filter_select",
                "targets": [{"datasetId": filter_dataset_id, "column": {"name": "geo_country"}}],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            },
            {
                "id": "device_filter",
                "name": "📱 Device Type",
                "filterType": "filter_select",
                "targets": [{"datasetId": filter_dataset_id, "column": {"name": "device_type"}}],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            },
            {
                "id": "browser_filter",
                "name": "🌐 Browser",
                "filterType": "filter_select",
                "targets": [{"datasetId": filter_dataset_id, "column": {"name": "browser_name"}}],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            }
        ]

    metadata = {
        "native_filter_configuration": native_filters,
        "color_scheme": "supersetColors",
        "label_colors": {}
    }
    return json.dumps(metadata)


def main() -> bool:
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("Creating E-commerce Analytics Dashboard")
    logger.info("=" * 60)

    # Импорты внутри main после создания app context
    from superset.app import create_app

    app = create_app()

    with app.app_context():
        from superset.extensions import db
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard
        from superset.connectors.sqla.models import SqlaTable

        created_charts = []
        datasets_by_name = {}

        # Создаём чарты
        for chart_config in CHARTS_CONFIG:
            dataset = db.session.query(SqlaTable).filter_by(
                table_name=chart_config["dataset_name"],
                schema="dm"
            ).first()

            if not dataset:
                logger.warning(f"Dataset '{chart_config['dataset_name']}' not found, skipping chart")
                continue

            if not dataset.columns:
                # Если dataset создали до DDL/DM, в metadata Superset нет колонок.
                # Обновляем их здесь, чтобы dashboard восстанавливался через make superset-dashboard.
                dataset.fetch_metadata()
                db.session.commit()
                logger.info("Refreshed dataset metadata: %s", dataset.table_name)

            datasets_by_name[chart_config["dataset_name"]] = dataset.id

            try:
                # Подготавливаем параметры
                params = chart_config["params"].copy()
                params["datasource"] = f"{dataset.id}__table"
                params["viz_type"] = chart_config["viz_type"]
                serialized_params = json.dumps(params)

                # Проверяем, существует ли уже чарт. Старые имена нужны для
                # идемпотентного rename без дублей в списке Charts.
                chart_names = [chart_config["slice_name"]]
                chart_names.extend(chart_config.get("previous_slice_names", []))
                existing = db.session.query(Slice).filter(
                    Slice.slice_name.in_(chart_names)
                ).order_by(Slice.id.asc()).first()

                if existing:
                    # Синхронизируем параметры существующего чарта с конфигом.
                    existing.slice_name = chart_config["slice_name"]
                    existing.viz_type = chart_config["viz_type"]
                    existing.datasource_id = dataset.id
                    existing.datasource_type = "table"
                    existing.datasource_name = dataset.table_name
                    existing.params = serialized_params
                    sync_query_context(existing, params, dataset.id)
                    existing.description = f"Chart created automatically for {chart_config['dataset_name']}"
                    db.session.flush()
                    logger.info(
                        f"Chart '{chart_config['slice_name']}' already exists (ID: {existing.id}), "
                        "params synced"
                    )
                    created_charts.append({"id": existing.id, "title": existing.slice_name})
                    continue

                # Создаём чарт
                chart = Slice(
                    slice_name=chart_config["slice_name"],
                    viz_type=chart_config["viz_type"],
                    datasource_id=dataset.id,
                    datasource_type="table",
                    datasource_name=dataset.table_name,
                    params=serialized_params,
                    description=f"Chart created automatically for {chart_config['dataset_name']}"
                )

                db.session.add(chart)
                db.session.flush()

                logger.info(f"Created chart: {chart_config['slice_name']} (ID: {chart.id})")
                created_charts.append({"id": chart.id, "title": chart.slice_name})

            except Exception as e:
                logger.error(f"Failed to create chart '{chart_config['slice_name']}': {e}")
                import traceback
                traceback.print_exc()
                db.session.rollback()

        logger.info(f"Created/Found {len(created_charts)} charts")

        current_chart_names = {chart_config["slice_name"] for chart_config in CHARTS_CONFIG}
        for obsolete_name in sorted(OBSOLETE_CHART_NAMES - current_chart_names):
            obsolete_charts = db.session.query(Slice).filter_by(slice_name=obsolete_name).all()
            for obsolete in obsolete_charts:
                db.session.delete(obsolete)
                logger.info("Deleted obsolete chart: %s (ID: %s)", obsolete_name, obsolete.id)
        db.session.flush()

        metadata_json = build_dashboard_metadata(datasets_by_name.get("v_events_enriched"))

        # Создаём позиции чартов для layout.
        # Обязательные блоки ROOT_ID/GRID_ID нужны для корректной работы /tabs.
        positions = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {
                "id": "ROOT_ID",
                "type": "ROOT",
                "children": ["GRID_ID"],
            },
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": [],
                "parents": ["ROOT_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
        }

        # Раскладываем dashboard строками (ROW): KPI в один ряд, затем
        # аналитические блоки парами. Каждый CHART обязан лежать внутри ROW,
        # иначе Superset игнорирует width и ставит чарты в одну колонку.
        charts_by_title = {c["title"]: c for c in created_charts if c}

        def add_chart(component_id_owner_row: str, chart: dict, width: int, height: int) -> str:
            """Регистрирует CHART-компонент и возвращает его id."""
            cid = f"CHART-{chart['id']}"
            positions[cid] = {
                "id": cid,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", component_id_owner_row],
                "meta": {
                    "chartId": chart["id"],
                    "sliceName": chart["title"],
                    "width": width,
                    "height": height,
                },
            }
            return cid

        placed_titles = set()
        for row_idx, row in enumerate(DASHBOARD_ROWS):
            row_id = f"ROW-{row_idx}"
            height = ROW_HEIGHTS[row_idx] if row_idx < len(ROW_HEIGHTS) else 50
            row_children = []
            for title, width in row:
                chart = charts_by_title.get(title)
                if not chart:
                    continue
                row_children.append(add_chart(row_id, chart, width, height))
                placed_titles.add(title)
            if row_children:
                positions[row_id] = {
                    "id": row_id,
                    "type": "ROW",
                    "children": row_children,
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"},
                }
                positions["GRID_ID"]["children"].append(row_id)

        # Чарты, не описанные в DASHBOARD_ROWS, кладём отдельной строкой во всю
        # ширину — чтобы новый чарт не «потерялся» из дашборда.
        leftovers = [c for c in created_charts if c and c["title"] not in placed_titles]
        if leftovers:
            row_id = "ROW-extra"
            row_children = [add_chart(row_id, c, 12, 50) for c in leftovers]
            positions[row_id] = {
                "id": row_id,
                "type": "ROW",
                "children": row_children,
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            positions["GRID_ID"]["children"].append(row_id)

        # Создаём дашборд
        if created_charts:
            try:
                # Проверяем, существует ли дашборд
                existing = db.session.query(Dashboard).filter_by(
                    slug=DASHBOARD_CONFIG["slug"]
                ).first()

                if existing:
                    existing.description = DASHBOARD_CONFIG["description"]
                    existing.published = DASHBOARD_CONFIG["published"]
                    existing.json_metadata = metadata_json
                    existing.position_json = json.dumps(positions)
                    existing.slices = []
                    for chart_info in created_charts:
                        chart = db.session.query(Slice).filter_by(id=chart_info["id"]).first()
                        if chart:
                            existing.slices.append(chart)
                    db.session.commit()
                    logger.info(f"Dashboard '{DASHBOARD_CONFIG['dashboard_title']}' already exists (ID: {existing.id})")
                    logger.info("=" * 60)
                    logger.info("Dashboard already exists and metadata/layout were updated.")
                    logger.info(f"Dashboard URL: /superset/dashboard/{existing.id}/")
                    logger.info("=" * 60)
                    return True

                # Создаём дашборд
                dashboard = Dashboard(
                    dashboard_title=DASHBOARD_CONFIG["dashboard_title"],
                    slug=DASHBOARD_CONFIG["slug"],
                    description=DASHBOARD_CONFIG["description"],
                    published=DASHBOARD_CONFIG["published"],
                    json_metadata=metadata_json,
                    position_json=json.dumps(positions)
                )

                db.session.add(dashboard)
                db.session.flush()

                # Добавляем чарты к дашборду
                for chart_info in created_charts:
                    if chart_info:
                        chart = db.session.query(Slice).filter_by(id=chart_info["id"]).first()
                        if chart:
                            dashboard.slices.append(chart)

                db.session.commit()

                logger.info(f"Created dashboard: {DASHBOARD_CONFIG['dashboard_title']} (ID: {dashboard.id})")
                logger.info("=" * 60)
                logger.info("Dashboard created successfully!")
                logger.info(f"Dashboard URL: /superset/dashboard/{dashboard.id}/")
                logger.info("=" * 60)
                return True

            except Exception as e:
                logger.error(f"Failed to create dashboard: {e}")
                import traceback
                traceback.print_exc()
                db.session.rollback()
                return False
        else:
            logger.error("No charts created, cannot create dashboard")
            return False

    return False

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
