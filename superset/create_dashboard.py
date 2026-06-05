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


# Конфигурация чартов
CHARTS_CONFIG = [
    # KPI блок
    {
        "slice_name": "📊 Total Events",
        "viz_type": "big_number",
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
            "granularity_sqla": "event_ts",
            "y_axis_format": ",d",
            "show_trend_line": False,
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "👤 Unique Users",
        "viz_type": "big_number",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(DISTINCT user_domain_id)",
                "label": "Unique Users",
                "optionName": "metric_2"
            },
            "granularity_sqla": "event_ts",
            "y_axis_format": ",d",
            "show_trend_line": False,
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "🎯 Unique Sessions",
        "viz_type": "big_number",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(DISTINCT click_id)",
                "label": "Unique Sessions",
                "optionName": "metric_3"
            },
            "granularity_sqla": "event_ts",
            "y_axis_format": ",d",
            "show_trend_line": False,
            "time_range": "No filter"
        }
    },
    {
        "slice_name": "📈 Avg Events/Session",
        "viz_type": "big_number",
        "dataset_name": "v_events_enriched",
        "params": {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*) / COUNT(DISTINCT click_id)",
                "label": "Avg Events/Session",
                "optionName": "metric_4"
            },
            "granularity_sqla": "event_ts",
            "y_axis_format": ".2f",
            "show_trend_line": False,
            "time_range": "No filter"
        }
    },
    # Динамика
    {
        "slice_name": "📅 Events by Hour",
        "viz_type": "echarts_timeseries_line",
        "dataset_name": "v_events_enriched",
        "params": {
            "granularity_sqla": "event_ts",
            "time_grain_sqla": "PT1H",
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
        "slice_name": "📄 Top Pages",
        "viz_type": "dist_bar",
        "dataset_name": "v_top_pages_daily",
        "params": {
            "groupby": ["page_url_path"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(pageviews)", "label": "Pageviews"}
            ],
            "row_limit": 20,
            "time_range": "No filter",
            "orientation": "vertical",
            "show_legend": False
        }
    },
    # Качество данных
    {
        "slice_name": "🔍 Data Quality Summary",
        "viz_type": "dist_bar",
        "dataset_name": "dq_summary",
        "params": {
            "groupby": ["layer"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(check_value)", "label": "Row Count"}
            ],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "expressionType": "SQL",
                    "sqlExpression": "check_name = 'total_rows'",
                    "subject": None,
                    "operator": None,
                    "comparator": None
                }
            ],
            "row_limit": 100,
            "time_range": "No filter",
            "show_legend": False
        }
    }
]

# Конфигурация дашборда
DASHBOARD_CONFIG = {
    "dashboard_title": "🛒 E-commerce Analytics Dashboard",
    "description": "Аналитический дашборд для e-commerce кликстрима: трафик, конверсии, география и качество данных.",
    "published": True,
    "slug": "ecommerce-analytics",
}


CHART_LAYOUT_BY_TITLE = {
    "📊 Total Events": {"width": 3, "height": 32, "x": 0, "y": 0},
    "👤 Unique Users": {"width": 3, "height": 32, "x": 3, "y": 0},
    "🎯 Unique Sessions": {"width": 3, "height": 32, "x": 6, "y": 0},
    "📈 Avg Events/Session": {"width": 3, "height": 32, "x": 9, "y": 0},
    "📅 Events by Hour": {"width": 8, "height": 60, "x": 0, "y": 32},
    "📱 Traffic by Device": {"width": 4, "height": 60, "x": 8, "y": 32},
    "🌍 Geography Map": {"width": 6, "height": 60, "x": 0, "y": 92},
    "🔗 UTM Effectiveness Table": {"width": 6, "height": 60, "x": 6, "y": 92},
    "📄 Top Pages": {"width": 6, "height": 60, "x": 0, "y": 152},
    "🔍 Data Quality Summary": {"width": 6, "height": 60, "x": 6, "y": 152},
}


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

    if chart.viz_type == "big_number":
        query = queries[0]
        granularity = params.get("granularity_sqla")
        if granularity:
            query["granularity"] = granularity
        query["is_timeseries"] = True
        query["time_range"] = params.get("time_range", query.get("time_range"))
        if "metric" in params:
            query["metrics"] = [params["metric"]]
        extras = query.get("extras") if isinstance(query.get("extras"), dict) else {}
        if "time_grain_sqla" in params:
            extras["time_grain_sqla"] = params.get("time_grain_sqla")
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

                # Проверяем, существует ли уже чарт
                existing = db.session.query(Slice).filter_by(
                    slice_name=chart_config["slice_name"]
                ).first()

                if existing:
                    # Синхронизируем параметры существующего чарта с конфигом.
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

        # Раскладываем dashboard вручную по 12-колоночной сетке:
        # KPI в одну строку, затем аналитические блоки парами.
        for chart in created_charts:
            if chart:
                chart_component_id = f"CHART-{chart['id']}"
                layout = CHART_LAYOUT_BY_TITLE.get(
                    chart["title"],
                    {"width": 6, "height": 50, "x": 0, "y": 212},
                )
                positions[chart_component_id] = {
                    "id": chart_component_id,
                    "type": "CHART",
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {
                        "chartId": chart['id'],
                        "sliceName": chart['title'],
                        "height": layout["height"],
                        "width": layout["width"],
                        "x": layout["x"],
                        "y": layout["y"],
                    },
                }
                positions["GRID_ID"]["children"].append(chart_component_id)

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
