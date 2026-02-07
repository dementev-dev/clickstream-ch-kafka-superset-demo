#!/usr/bin/env python3
"""
================================================================================
Скрипт инициализации Superset для проекта ClickHouse Mini DWH
================================================================================
Назначение:
    - Создание подключения к ClickHouse (Database connection)
    - Импорт датасетов из витрин DM-слоя

Запуск:
    Внутри контейнера superset:
    python /app/superset_init/init_superset.py

Важно:
    Superset использует SQLite по умолчанию (не PostgreSQL).
    Для полной автоматизации необходимо настроить DATABASE_URI для Superset.
    
    Текущий подход: используем Superset CLI для создания подключения.

Требования:
    - Запущенный ClickHouse с созданными витринами в схеме dm
    - Superset инициализирован (superset db upgrade, admin создан)
================================================================================
"""

import os
import sys
import json
import logging
import subprocess
from typing import Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем путь к superset
sys.path.insert(0, '/app')


def run_superset_cli(args):
    """Запуск команды superset CLI"""
    cmd = ['superset'] + args
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command failed: {result.stderr}")
    else:
        logger.info(f"Command output: {result.stdout}")
    return result.returncode == 0, result.stdout, result.stderr


def create_clickhouse_connection():
    """Создание подключения к ClickHouse через CLI"""
    logger.info("Creating ClickHouse database connection...")
    
    # Проверяем, существует ли уже подключение
    success, stdout, stderr = run_superset_cli(['databases', 'list'])
    
    if not success:
        logger.warning(f"Could not list databases: {stderr}")
    elif 'clickhouse_dwh' in stdout:
        logger.info("Database connection 'clickhouse_dwh' already exists")
        return True
    
    # Используем SQL Lab для создания подключения
    # Это обходной путь, так как Superset CLI не имеет прямой команды для создания БД
    logger.info("Database connection needs to be created manually via UI")
    logger.info("Go to: Settings → Database Connections → + Database")
    logger.info("Select: ClickHouse")
    logger.info("URI: clickhouse+native://default@clickhouse:9000/default")
    
    return True


def import_datasets():
    """Импорт датасетов через Superset Python API"""
    logger.info("Importing datasets...")
    
    try:
        from superset.app import create_app
        from superset.extensions import db
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        
        app = create_app()
    except Exception as e:
        logger.error(f"Failed to import Superset modules: {e}")
        logger.info("Please ensure Superset is properly initialized")
        return False
    
    datasets = [
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
    
    with app.app_context():
        # Получаем ID базы данных
        database = db.session.query(Database).filter_by(
            database_name="clickhouse_dwh"
        ).first()
        
        if not database:
            logger.error("ClickHouse database connection not found!")
            logger.info("Please create database connection manually first:")
            logger.info("1. Go to http://localhost:8088")
            logger.info("2. Login: admin / admin")
            logger.info("3. Settings → Database Connections → + Database")
            logger.info("4. Select ClickHouse")
            logger.info("5. URI: clickhouse+native://default@clickhouse:9000/default")
            return False
        
        imported_count = 0
        for dataset_config in datasets:
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
                try:
                    dataset.fetch_metadata()
                except Exception as e:
                    logger.warning(f"Could not fetch metadata for {dataset_config['table_name']}: {e}")
                
                db.session.commit()
                logger.info(f"Successfully imported dataset: {dataset_config['table_name']}")
                imported_count += 1
                
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
    
    # Проверяем подключение к ClickHouse
    if not create_clickhouse_connection():
        logger.error("Failed to verify ClickHouse connection")
        sys.exit(1)
    
    # Импортируем датасеты
    try:
        if not import_datasets():
            logger.error("Failed to import datasets")
            logger.info("\nTo create datasets manually:")
            logger.info("1. Go to http://localhost:8088")
            logger.info("2. Datasets → + Dataset")
            logger.info("3. Select 'clickhouse_dwh' database")
            logger.info("4. Select schema 'dm' and desired table")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error importing datasets: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Superset initialization completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
