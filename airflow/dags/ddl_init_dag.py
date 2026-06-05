"""
DAG инициализации DDL в ClickHouse.

Учебный формат:
- каждая операция DDL выполняется отдельной SQL-task;
- SQL-файлы вызываются явно по фиксированным путям;
- режим verify_only позволяет прогонять только проверки схемы.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from utils.airflow_params import parse_bool_param
from utils.sql_helpers import load_sql_statements as load_sql_file_statements


# -----------------------------------------------------------------------------
# Базовые настройки DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# -----------------------------------------------------------------------------
# SQL-файлы проекта
# -----------------------------------------------------------------------------
def resolve_sql_root() -> Path:
    """Определяет корень SQL для контейнера и локального запуска."""
    candidates = (
        Path(__file__).resolve().parents[1] / "sql",  # /opt/airflow/sql в контейнере
        Path(__file__).resolve().parents[2] / "sql",  # <repo>/sql при локальном запуске
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


SQL_ROOT = resolve_sql_root()


def load_sql_statements(relative_path: str) -> tuple[str, ...]:
    """Читает SQL-файл и делит его на отдельные команды по ';'."""
    return load_sql_file_statements(SQL_ROOT, relative_path)


# -----------------------------------------------------------------------------
# SQL-проверки
# -----------------------------------------------------------------------------
SQL_CHECK_CLICKHOUSE = "SELECT 1 AS ok"

SQL_VERIFY_SCHEMA = """
SELECT
    (SELECT count() FROM system.tables WHERE database = 'stg' AND name = 'browser_raw') AS stg_browser_raw,
    (SELECT count() FROM system.tables WHERE database = 'ods' AND name = 'browser_event') AS ods_browser_event,
    (SELECT count() FROM system.tables WHERE database = 'dds' AND name = 'click') AS dds_click,
    (SELECT count() FROM system.tables WHERE database = 'dds' AND name = 'event') AS dds_event,
    (SELECT count() FROM system.tables WHERE database = 'dm' AND name = 'v_events_enriched') AS dm_v_events_enriched
"""


# -----------------------------------------------------------------------------
# Управляющие функции
# -----------------------------------------------------------------------------
def choose_ddl_mode(**context) -> str:
    """Выбирает ветку выполнения: full DDL или только verify."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    verify_only = parse_bool_param(
        conf.get("verify_only", context["params"]["verify_only"]),
        "verify_only",
    )
    return "skip_ddl" if verify_only else "ddl_00_databases"


def assert_schema_ready(**context) -> None:
    """Проверяет результат финальной SQL-проверки схемы."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="verify_schema_sql")

    if not result or not result[0] or len(result[0]) != 5:
        raise AirflowException(f"Некорректный результат проверки схемы: {result}")

    if any(value == 0 for value in result[0]):
        raise AirflowException(
            "Схема применена не полностью. Проверьте таблицы/VIEW stg, ods, dds, dm."
        )


with DAG(
    dag_id="ddl_init",
    description="Инициализация схемы ClickHouse (stg/ods/dds/dm)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ddl", "bootstrap", "clickhouse"],
    params={
        "verify_only": Param(False, type="boolean"),
    },
) as dag:
    check_clickhouse = ClickHouseOperator(
        task_id="check_clickhouse",
        sql=SQL_CHECK_CLICKHOUSE,
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    choose_mode = BranchPythonOperator(
        task_id="choose_mode",
        python_callable=choose_ddl_mode,
    )

    ddl_00_databases = ClickHouseOperator(
        task_id="ddl_00_databases",
        sql=load_sql_statements("ddl/00_databases.sql"),
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    ddl_10_stg = ClickHouseOperator(
        task_id="ddl_10_stg",
        sql=load_sql_statements("ddl/stg/10_stg.sql"),
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    ddl_20_ods = ClickHouseOperator(
        task_id="ddl_20_ods",
        sql=load_sql_statements("ddl/ods/20_ods.sql"),
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    ddl_30_dds = ClickHouseOperator(
        task_id="ddl_30_dds",
        sql=load_sql_statements("ddl/dds/30_dds.sql"),
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    ddl_40_dm = ClickHouseOperator(
        task_id="ddl_40_dm",
        sql=load_sql_statements("ddl/dm/40_dm.sql"),
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    skip_ddl = EmptyOperator(task_id="skip_ddl")

    ddl_complete = EmptyOperator(
        task_id="ddl_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    verify_schema_sql = ClickHouseOperator(
        task_id="verify_schema_sql",
        sql=SQL_VERIFY_SCHEMA,
        clickhouse_conn_id="clickhouse_default",
        database="default",
    )

    verify_schema = PythonOperator(
        task_id="verify_schema",
        python_callable=assert_schema_ready,
    )

    check_clickhouse >> choose_mode
    choose_mode >> skip_ddl >> ddl_complete
    choose_mode >> ddl_00_databases >> ddl_10_stg >> ddl_20_ods >> ddl_30_dds >> ddl_40_dm >> ddl_complete
    ddl_complete >> verify_schema_sql >> verify_schema
