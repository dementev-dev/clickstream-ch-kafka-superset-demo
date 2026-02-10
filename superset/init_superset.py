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
    
    Текущий подход: 
    1. CLI для создания подключения к БД
    2. Superset shell для импорта датасетов (требуется app context)

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
    
    # Создаем подключение через set-database-uri
    # clickhouse-connect использует HTTP порт 8123 внутри Docker сети
    success, stdout, stderr = run_superset_cli([
        'set-database-uri',
        '-d', 'clickhouse_dwh',
        '-u', 'clickhouse+connect://default@clickhouse:8123/default'
    ])
    
    if success:
        logger.info("Successfully created ClickHouse database connection 'clickhouse_dwh'")
        return True
    else:
        logger.error(f"Failed to create database connection: {stderr}")
        logger.info("Please create manually via UI:")
        logger.info("1. Go to: Settings → Database Connections → + Database")
        logger.info("2. Select: ClickHouse")
        logger.info("3. URI: clickhouse+connect://default@clickhouse:8123/default")
        return False


def import_datasets():
    """Импорт датасетов через Superset shell"""
    logger.info("Importing datasets...")
    
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
    
    # Создаем Python скрипт для выполнения внутри superset shell
    script_lines = [
        "from superset.extensions import db",
        "from superset.models.core import Database",
        "from superset.connectors.sqla.models import SqlaTable",
        "",
        "# Получаем базу данных",
        "database = db.session.query(Database).filter_by(database_name='clickhouse_dwh').first()",
        "if not database:",
        "    print('ERROR: Database clickhouse_dwh not found')",
        "    exit(1)",
        "",
        "print(f'Found database: {database.database_name} (id={database.id})')",
        "",
        "imported = 0",
    ]
    
    for ds in datasets:
        script_lines.extend([
            "",
            f"# Dataset: {ds['table_name']}",
            f"existing = db.session.query(SqlaTable).filter_by(table_name='{ds['table_name']}', schema='{ds['schema']}').first()",
            "if existing:",
            f"    print(f'Dataset {ds['table_name']} already exists')",
            "else:",
            "    try:",
            f"        dataset = SqlaTable(table_name='{ds['table_name']}', schema='{ds['schema']}', database_id=database.id, database=database, description='{ds['description']}')",
            "        db.session.add(dataset)",
            "        db.session.flush()",
            f"        print(f'Created dataset: {ds['table_name']}')",
            "        imported += 1",
            "    except Exception as e:",
            f"        print(f'Error creating {ds['table_name']}: {{e}}')",
            "        db.session.rollback()",
        ])
    
    script_lines.extend([
        "",
        "db.session.commit()",
        "print(f'Successfully imported {imported} datasets')",
    ])
    
    script_content = '\n'.join(script_lines)
    
    # Запускаем через superset shell
    cmd = ['superset', 'shell']
    logger.info("Running datasets import via superset shell...")
    
    result = subprocess.run(
        cmd,
        input=script_content,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"Shell command failed: {result.stderr}")
        return False
    
    logger.info(f"Shell output:\n{result.stdout}")
    if "ERROR" in result.stdout:
        logger.error("Failed to import some datasets")
        return False
    
    logger.info("Datasets imported successfully")
    return True


def main():
    """Главная функция инициализации"""
    logger.info("=" * 60)
    logger.info("Superset Initialization for ClickHouse Mini DWH")
    logger.info("=" * 60)
    
    # Создаем подключение к ClickHouse
    if not create_clickhouse_connection():
        logger.error("Failed to create ClickHouse connection")
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
