"""
ETL Pipeline DAG для ClickHouse DWH

Шаблон DAG для оркестрации пайплайна данных.
Полная реализация будет добавлена позже.

Пайплайн:
    1. DDL - создание структуры БД
    2. Load - загрузка данных в Kafka
    3. Transform - batch трансформация ODS → DDS → DM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Базовые настройки DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL pipeline для ClickHouse DWH",
    schedule=None,  # Запуск только вручную (пока)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "clickhouse", "dwh"],
) as dag:
    
    # TODO: добавить задачи пайплайна
    # - ddl: создание структуры БД
    # - load: загрузка данных в Kafka  
    # - transform: batch трансформация
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    start >> end
