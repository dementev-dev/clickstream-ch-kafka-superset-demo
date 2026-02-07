#!/usr/bin/env python3
"""
================================================================================
Скрипт инициализации Superset для проекта ClickHouse Mini DWH
================================================================================
Назначение:
    - Создание подключения к ClickHouse (Database connection)
    - Импорт датасетов из витрин DM-слоя
    - Импорт чартов и дашбордов

Запуск:
    Внутри контейнера superset:
    python /app/superset_init/init_superset.py

Требования:
    - Запущенный ClickHouse с созданными витринами в схеме dm
    - Superset инициализирован (superset db upgrade, admin создан)
================================================================================
"""

import os
import sys
import json
import logging
from typing import Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем путь к superset
sys.path.insert(0, '/app')

try:
    from superset.app import create_app
    from superset.extensions import db
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.charts.data_access_layer import ChartDAO
    from superset.dashboards.data_access_layer import DashboardDAO
    from superset.commands.dataset.create import CreateDatasetCommand
    from sqlalchemy.exc import IntegrityError
except ImportError as e:
    logger.error(f"Failed to import Superset modules: {e}")
    sys.exit(1)

# Конфигурация подключения к ClickHouse
CLICKHOUSE_CONFIG = {
    "database_name": "clickhouse_dwh",
    "sqlalchemy_uri": "clickhouse+native://default@clickhouse:9000/default",
    "expose_in_sqllab": True,
    "allow_ctas": False,
    "allow_cvas": False,
    "allow_dml": False,
    "allow_file_upload": False,
    "extra": json.dumps({
        "engine_params": {},
        "metadata_params": {},
        "schemas_allowed_for_file_upload": []
    })
}

# Датасеты для импорта из DM-слоя
DATASETS = [
    {
        "table_name": "v_events_enriched",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Полная обогащённая витрина событий (event + click)"
    },
    {
        "table_name": "v_daily_traffic",
        "schema": "dm", 
        "database_name": "clickhouse_dwh",
        "description": "Агрегация трафика по дням и измерениям"
    },
    {
        "table_name": "v_utm_effectiveness",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Эффективность UTM-кампаний"
    },
    {
        "table_name": "v_top_pages_daily",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Популярность страниц по дням"
    },
    {
        "table_name": "v_session_overview",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Обзор сессий пользователей"
    },
    {
        "table_name": "dq_summary",
        "schema": "dm",
        "database_name": "clickhouse_dwh",
        "description": "Сводка по качеству данных"
    }
]


def create_clickhouse_connection(app) -> Optional[Database]:
    """Создание подключения к ClickHouse"""
    with app.app_context():
        logger.info("Creating ClickHouse database connection...")
        
        # Проверяем, существует ли уже подключение
        existing = db.session.query(Database).filter_by(
            database_name=CLICKHOUSE_CONFIG["database_name"]
        ).first()
        
        if existing:
            logger.info(f"Database connection '{CLICKHOUSE_CONFIG['database_name']}' already exists")
            return existing
        
        try:
            database = Database(**CLICKHOUSE_CONFIG)
            db.session.add(database)
            db.session.commit()
            logger.info(f"Successfully created database connection: {CLICKHOUSE_CONFIG['database_name']}")
            return database
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to create database connection: {e}")
            return None


def import_datasets(app):
    """Импорт датасетов из DM-слоя"""
    with app.app_context():
        logger.info("Importing datasets...")
        
        # Получаем ID базы данных
        database = db.session.query(Database).filter_by(
            database_name="clickhouse_dwh"
        ).first()
        
        if not database:
            logger.error("ClickHouse database connection not found")
            return False
        
        imported_count = 0
        for dataset_config in DATASETS:
            try:
                # Проверяем, существует ли датасет
                existing = db.session.query(SqlaTable).filter_by(
                    table_name=dataset_config["table_name"],
                    schema=dataset_config["schema"]
                ).first()
                
                if existing:
                    logger.info(f"Dataset '{dataset_config['table_name']}' already exists")
                    continue
                
                # Создаём датасет
                dataset = SqlaTable(
                    table_name=dataset_config["table_name"],
                    schema=dataset_config["schema"],
                    database_id=database.id,
                    database=database,
                    description=dataset_config["description"],
                    is_sqllab_view=False
                )
                
                db.session.add(dataset)
                db.session.flush()
                
                # Fetch columns from database
                dataset.fetch_metadata()
                
                db.session.commit()
                logger.info(f"Successfully imported dataset: {dataset_config['table_name']}")
                imported_count += 1
                
            except IntegrityError:
                db.session.rollback()
                logger.warning(f"Dataset '{dataset_config['table_name']}' already exists (integrity error)")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Failed to import dataset '{dataset_config['table_name']}': {e}")
        
        logger.info(f"Imported {imported_count} new datasets")
        return True


def main():
    """Главная функция инициализации"""
    logger.info("=" * 60)
    logger.info("Superset Initialization for ClickHouse Mini DWH")
    logger.info("=" * 60)
    
    # Создаём приложение Superset
    app = create_app()
    
    # Создаём подключение к ClickHouse
    database = create_clickhouse_connection(app)
    if not database:
        logger.error("Failed to create ClickHouse connection")
        sys.exit(1)
    
    # Импортируем датасеты
    if not import_datasets(app):
        logger.error("Failed to import datasets")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Superset initialization completed successfully!")
    logger.info("=" * 60)
    logger.info("Available datasets:")
    for ds in DATASETS:
        logger.info(f"  - {ds['schema']}.{ds['table_name']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
