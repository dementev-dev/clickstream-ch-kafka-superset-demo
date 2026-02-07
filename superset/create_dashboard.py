#!/usr/bin/env python3
"""
================================================================================
Скрипт создания дашборда "E-commerce Analytics" в Superset
================================================================================
Назначение:
    - Создание чартов (Charts) на основе датасетов DM-слоя
    - Создание дашборда с layout и фильтрами
    - Настройка native filters

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
            "time_range": "Last week",
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
        "viz_type": "echarts_bar",
        "dataset_name": "v_top_pages_daily",
        "params": {
            "x_axis": "page_url_path",
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(pageviews)", "label": "Pageviews"}
            ],
            "row_limit": 20,
            "order_by_cols": [["SUM(pageviews)", False]],
            "time_range": "No filter",
            "orientation": "vertical",
            "show_legend": False
        }
    },
    # Качество данных
    {
        "slice_name": "🔍 Data Quality Summary",
        "viz_type": "echarts_bar",
        "dataset_name": "dq_summary",
        "params": {
            "x_axis": "layer",
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
    "json_metadata": json.dumps({
        "native_filter_configuration": [
            {
                "id": "date_filter",
                "name": "📅 Date Range",
                "filterType": "filter_time",
                "targets": [{"datasetId": None, "column": {"name": "event_date"}}],
                "defaultValue": "Last week",
                "scope": {"root": ["ROOT_ID"], "excluded": []},
                "cascadeParentIds": [],
                "isInstant": True
            },
            {
                "id": "country_filter",
                "name": "🌍 Country",
                "filterType": "filter_select",
                "targets": [{"datasetId": None, "column": {"name": "geo_country"}}],
                "scope": {"root": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            },
            {
                "id": "device_filter",
                "name": "📱 Device Type",
                "filterType": "filter_select",
                "targets": [{"datasetId": None, "column": {"name": "device_type"}}],
                "scope": {"root": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            },
            {
                "id": "browser_filter",
                "name": "🌐 Browser",
                "filterType": "filter_select",
                "targets": [{"datasetId": None, "column": {"name": "browser_name"}}],
                "scope": {"root": ["ROOT_ID"], "excluded": []},
                "isInstant": True,
                "allowsMultipleValues": True,
                "isRequired": False
            }
        ],
        "color_scheme": "supersetColors",
        "label_colors": {}
    })
}


def get_dataset_by_name(app, dataset_name: str):
    """Получение датасета по имени таблицы"""
    from superset.connectors.sqla.models import SqlaTable
    
    with app.app_context():
        dataset = db.session.query(SqlaTable).filter_by(
            table_name=dataset_name,
            schema="dm"
        ).first()
        return dataset


def create_chart(app, chart_config: dict, dataset):
    """Создание чарта"""
    from superset.extensions import db
    from superset.utils.core import DatasourceType
    from superset.charts.commands.create import CreateChartCommand
    
    with app.app_context():
        try:
            # Подготавливаем параметры
            params = chart_config["params"].copy()
            params["datasource"] = f"{dataset.id}__{DatasourceType.TABLE.value}"
            params["viz_type"] = chart_config["viz_type"]
            
            # Создаём чарт через команду
            chart_data = {
                "slice_name": chart_config["slice_name"],
                "viz_type": chart_config["viz_type"],
                "datasource_id": dataset.id,
                "datasource_type": DatasourceType.TABLE.value,
                "params": json.dumps(params),
                "description": f"Chart created automatically for {chart_config['dataset_name']}"
            }
            
            result = CreateChartCommand(chart_data).run()
            logger.info(f"Created chart: {chart_config['slice_name']} (ID: {result.id})")
            return {"id": result.id, "title": result.slice_name}
            
        except Exception as e:
            logger.error(f"Failed to create chart '{chart_config['slice_name']}': {e}")
            import traceback
            traceback.print_exc()
            return None


def create_dashboard(app, charts: list):
    """Создание дашборда с чартами"""
    from superset.extensions import db
    from superset.dashboards.commands.create import CreateDashboardCommand
    from superset.dashboards.dao import DashboardDAO
    from superset.charts.dao import ChartDAO
    
    with app.app_context():
        try:
            # Проверяем, существует ли дашборд
            existing = DashboardDAO.get_by_slug(DASHBOARD_CONFIG["slug"])
            
            if existing:
                logger.info(f"Dashboard '{DASHBOARD_CONFIG['dashboard_title']}' already exists")
                return existing
            
            # Создаём позиции чартов для layout
            positions = {
                "DASHBOARD_VERSION_KEY": "v2"
            }
            
            # Добавляем чарты в layout (grid: 12 columns)
            y_position = 0
            chart_index = 0
            
            for chart in charts:
                if chart:
                    positions[f"CHART-{chart['id']}"] = {
                        "id": f"CHART-{chart['id']}",
                        "type": "CHART",
                        "parents": ["ROOT_ID"],
                        "meta": {
                            "chartId": chart['id'],
                            "sliceName": chart['title'],
                            "height": 50,
                            "width": 4 if chart_index < 4 else 6,
                            "x": (chart_index % 3) * 4 if chart_index < 4 else (chart_index % 2) * 6,
                            "y": y_position
                        }
                    }
                    chart_index += 1
                    if chart_index % 4 == 0:
                        y_position += 50
            
            # Создаём дашборд
            dashboard_data = {
                "dashboard_title": DASHBOARD_CONFIG["dashboard_title"],
                "slug": DASHBOARD_CONFIG["slug"],
                "description": DASHBOARD_CONFIG["description"],
                "published": DASHBOARD_CONFIG["published"],
                "json_metadata": DASHBOARD_CONFIG["json_metadata"],
                "position_json": json.dumps(positions)
            }
            
            result = CreateDashboardCommand(dashboard_data).run()
            
            # Добавляем чарты к дашборду
            dashboard = DashboardDAO.get_by_id(result.id)
            
            for chart_info in charts:
                if chart_info:
                    chart = ChartDAO.find_by_id(chart_info["id"])
                    if chart:
                        dashboard.slices.append(chart)
            
            db.session.commit()
            
            logger.info(f"Created dashboard: {DASHBOARD_CONFIG['dashboard_title']} (ID: {result.id})")
            return result
            
        except Exception as e:
            logger.error(f"Failed to create dashboard: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("Creating E-commerce Analytics Dashboard")
    logger.info("=" * 60)
    
    from superset.app import create_app
    from superset.extensions import db
    
    app = create_app()
    
    created_charts = []
    
    # Создаём чарты
    for chart_config in CHARTS_CONFIG:
        dataset = get_dataset_by_name(app, chart_config["dataset_name"])
        if not dataset:
            logger.warning(f"Dataset '{chart_config['dataset_name']}' not found, skipping chart")
            continue
        
        chart = create_chart(app, chart_config, dataset)
        if chart:
            created_charts.append(chart)
    
    logger.info(f"Created {len(created_charts)} charts")
    
    # Создаём дашборд
    if created_charts:
        dashboard = create_dashboard(app, created_charts)
        if dashboard:
            logger.info("=" * 60)
            logger.info("Dashboard created successfully!")
            logger.info(f"Dashboard URL: /superset/dashboard/{dashboard.id}/")
            logger.info("=" * 60)
        else:
            logger.error("Failed to create dashboard")
    else:
        logger.error("No charts created, cannot create dashboard")


if __name__ == "__main__":
    main()
