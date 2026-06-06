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
    Superset использует PostgreSQL metadata store через configs/superset_config.py.
    
    Текущий подход: 
    1. CLI для создания подключения к БД
    2. Superset shell для импорта датасетов (требуется app context)

Требования:
    - Запущенный ClickHouse с созданными витринами в схеме dm
    - Superset инициализирован (superset db upgrade, admin создан)
================================================================================
"""

import sys
import os
import logging
import subprocess
from urllib.parse import quote_plus
from sqlalchemy.exc import NoSuchTableError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем путь к superset
sys.path.insert(0, '/app')

CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '123456')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_HTTP_PORT', '8123')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'default')


def is_expected_missing_table_error(exc: Exception, schema_name: str, table_name: str) -> bool:
    """Проверяет, что ошибка связана с отсутствием таблицы/вьюхи в ClickHouse."""
    if isinstance(exc, NoSuchTableError):
        return True

    text = str(exc).strip().lower()
    full_table_name = f"{schema_name}.{table_name}".lower()
    known_markers = (
        "doesn't exist",
        "does not exist",
        "unknown table",
        "no such table",
        "code: 60",
    )

    if text == full_table_name:
        return True

    return full_table_name in text and any(marker in text for marker in known_markers)


def build_clickhouse_uri() -> str:
    """Собирает URI подключения к ClickHouse для Superset."""
    user = quote_plus(CLICKHOUSE_USER)
    password = quote_plus(CLICKHOUSE_PASSWORD)
    host = CLICKHOUSE_HOST
    port = CLICKHOUSE_PORT
    database = quote_plus(CLICKHOUSE_DATABASE)
    return f"clickhousedb://{user}:{password}@{host}:{port}/{database}"


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
    database_uri = build_clickhouse_uri()
    
    # Создаем подключение через set-database-uri
    # clickhouse-connect использует HTTP порт 8123 внутри Docker сети
    success, stdout, stderr = run_superset_cli([
        'set-database-uri',
        '-d', 'clickhouse_dwh',
        '-u', database_uri
    ])
    
    if success:
        logger.info("Successfully created ClickHouse database connection 'clickhouse_dwh'")
        return True
    else:
        logger.error(f"Failed to create database connection: {stderr}")
        logger.info("Please create manually via UI:")
        logger.info("1. Go to: Settings → Database Connections → + Database")
        logger.info("2. Select: ClickHouse")
        logger.info(f"3. URI: {database_uri}")
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
        "import clickhouse_connect  # Регистрирует диалект clickhousedb",
        "from superset.extensions import db",
        "from superset.models.core import Database",
        "from superset.connectors.sqla.models import SqlaTable",
        "from sqlalchemy.exc import NoSuchTableError",
        "",
        "def is_expected_missing_table_error(exc, schema_name, table_name):",
        "    text = str(exc).strip().lower()",
        "    full_table_name = f'{schema_name}.{table_name}'.lower()",
        "    known_markers = ('doesn\\'t exist', 'does not exist', 'unknown table', 'no such table', 'code: 60')",
        "    if isinstance(exc, NoSuchTableError):",
        "        return True",
        "    if text == full_table_name:",
        "        return True",
        "    return full_table_name in text and any(marker in text for marker in known_markers)",
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
        "refreshed = 0",
        "errors = 0",
    ]
    
    for ds in datasets:
        table_name = ds["table_name"]
        schema_name = ds["schema"]
        description = ds["description"]
        script_lines.extend([
            "",
            f"# Dataset: {table_name}",
            (
                "existing = db.session.query(SqlaTable).filter_by("
                f"table_name={table_name!r}, schema={schema_name!r}"
                ").first()"
            ),
            "if existing:",
            "    try:",
            "        if not existing.columns:",
            "            existing.fetch_metadata()",
            "            db.session.commit()",
            f"            print('Refreshed dataset metadata: {table_name}')",
            "            refreshed += 1",
            "        else:",
            f"            print('Dataset {table_name} already exists')",
            "    except Exception as e:",
            f"        if is_expected_missing_table_error(e, {schema_name!r}, {table_name!r}):",
            f"            print('WARNING: metadata not refreshed for {table_name}: source table/view not found yet')",
            "            db.session.rollback()",
            "        else:",
            f"            print(f'ERROR: failed to refresh {table_name}: {{e}}')",
            "            db.session.rollback()",
            "            errors += 1",
            "else:",
            "    try:",
            (
                "        dataset = SqlaTable("
                f"table_name={table_name!r}, "
                f"schema={schema_name!r}, "
                "database_id=database.id, "
                "database=database, "
                f"description={description!r}"
                ")"
            ),
            "        db.session.add(dataset)",
            "        db.session.flush()",
            "        dataset.fetch_metadata()",
            "        db.session.commit()",
            f"        print('Created dataset: {table_name}')",
            "        imported += 1",
            "    except Exception as e:",
            f"        if is_expected_missing_table_error(e, {schema_name!r}, {table_name!r}):",
            "            db.session.commit()",
            f"            print('WARNING: created dataset {table_name} without metadata: source table/view not found yet')",
            "            imported += 1",
            "        else:",
            f"            print(f'ERROR: failed to create {table_name}: {{e}}')",
            "            db.session.rollback()",
            "            errors += 1",
        ])
    
    script_lines.extend([
        "",
        "print(f'Successfully imported {imported} datasets')",
        "print(f'Refreshed metadata for {refreshed} datasets')",
        "print(f'Errors: {errors}')",
        "if errors > 0:",
        "    raise SystemExit(1)",
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
