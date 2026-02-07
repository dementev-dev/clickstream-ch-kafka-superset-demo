#!/usr/bin/env python3
"""
================================================================================
Экспорт дашборда Superset в JSON-формат
================================================================================
Назначение:
    - Экспорт созданного дашборда в JSON для версионирования
    - Формат совместимый с superset import-dashboards

Запуск:
    docker compose exec superset python /app/superset_init/export_dashboard.py
================================================================================
"""

import json
import sys
import os
sys.path.insert(0, '/app')

try:
    from superset.app import create_app
    from superset.dashboards.data_access_layer import DashboardDAO
    from superset.charts.data_access_layer import ChartDAO
except ImportError as e:
    print(f"Error importing: {e}")
    sys.exit(1)


def export_dashboard(slug: str, output_path: str):
    """Экспорт дашборда в JSON"""
    app = create_app()
    
    with app.app_context():
        dashboard = DashboardDAO.get_by_slug(slug)
        
        if not dashboard:
            print(f"Dashboard with slug '{slug}' not found")
            return False
        
        # Собираем данные дашборда
        dashboard_data = {
            "dashboards": [
                {
                    "__Dashboard__": {
                        "dashboard_title": dashboard.dashboard_title,
                        "description": dashboard.description,
                        "slug": dashboard.slug,
                        "json_metadata": dashboard.json_metadata,
                        "position_json": dashboard.position_json,
                        "published": dashboard.published,
                        "slices": []
                    }
                }
            ],
            "charts": [],
            "datasets": []
        }
        
        # Добавляем чарты
        for slice_obj in dashboard.slices:
            chart_data = {
                "__Slice__": {
                    "slice_name": slice_obj.slice_name,
                    "viz_type": slice_obj.viz_type,
                    "params": slice_obj.params,
                    "description": slice_obj.description,
                    "datasource_type": slice_obj.datasource_type,
                    "datasource_name": slice_obj.datasource.name if slice_obj.datasource else None
                }
            }
            dashboard_data["dashboards"][0]["__Dashboard__"]["slices"].append(slice_obj.id)
            dashboard_data["charts"].append(chart_data)
            
            # Добавляем датасет
            if slice_obj.datasource:
                ds = slice_obj.datasource
                dataset_data = {
                    "__SqlaTable__": {
                        "table_name": ds.table_name,
                        "schema": ds.schema,
                        "database": ds.database.database_name if ds.database else None,
                        "description": ds.description,
                        "columns": [
                            {
                                "column_name": col.column_name,
                                "type": col.type,
                                "description": col.description
                            }
                            for col in ds.columns
                        ]
                    }
                }
                # Добавляем уникальные датасеты
                if dataset_data not in dashboard_data["datasets"]:
                    dashboard_data["datasets"].append(dataset_data)
        
        # Сохраняем в файл
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
        
        print(f"Dashboard exported to: {output_path}")
        return True


if __name__ == "__main__":
    output_file = "/app/superset_init/dashboards/ecommerce_analytics.json"
    export_dashboard("ecommerce-analytics", output_file)
