"""
DAG ETL-процесса STG -> ODS -> DDS -> DM для учебного проекта.

Поток задач:
  precheck -> transform: wait -> ods -> dq -> branch -> dds -> integrity -> dm -> validate

Принципы реализации:
- SQL выполняется явными task на ClickHouseOperator;
- SQL-файлы вызываются по фиксированным путям;
- Python используется только для управляющей логики (branch/wait/assert).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
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
# SQL для проверок и технических шагов
# -----------------------------------------------------------------------------
SQL_CHECK_CLICKHOUSE = "SELECT 1 AS ok"

SQL_CHECK_SCHEMA_READY = """
SELECT
    (SELECT count() FROM system.tables WHERE database = 'stg' AND name = 'browser_raw') AS stg_browser_raw,
    (SELECT count() FROM system.tables WHERE database = 'ods' AND name = 'browser_event') AS ods_browser_event,
    (SELECT count() FROM system.tables WHERE database = 'dds' AND name = 'event') AS dds_event,
    (SELECT count() FROM system.tables WHERE database = 'dm' AND name = 'v_events_enriched') AS dm_v_events_enriched
"""

SQL_CHECK_ODS_QUALITY = """
SELECT
    table_name,
    total_rows,
    rows_with_errors,
    round(if(total_rows = 0, 0, rows_with_errors / total_rows * 100), 2) AS error_pct
FROM
(
    SELECT
        'browser_event' AS table_name,
        toFloat64(count()) AS total_rows,
        toFloat64(countIf(length(parse_errors) > 0)) AS rows_with_errors
    FROM ods.browser_event
    UNION ALL
    SELECT
        'location_event',
        toFloat64(count()),
        toFloat64(countIf(length(parse_errors) > 0))
    FROM ods.location_event
    UNION ALL
    SELECT
        'device_by_click',
        toFloat64(count()),
        toFloat64(countIf(length(parse_errors) > 0))
    FROM ods.device_by_click
    UNION ALL
    SELECT
        'geo_by_click',
        toFloat64(count()),
        toFloat64(countIf(length(parse_errors) > 0))
    FROM ods.geo_by_click
    UNION ALL
    SELECT
        'browser_event_errors',
        toFloat64(count()),
        toFloat64(count())
    FROM ods.browser_event_errors
    UNION ALL
    SELECT
        'location_event_errors',
        toFloat64(count()),
        toFloat64(count())
    FROM ods.location_event_errors
    UNION ALL
    SELECT
        'device_by_click_errors',
        toFloat64(count()),
        toFloat64(count())
    FROM ods.device_by_click_errors
    UNION ALL
    SELECT
        'geo_by_click_errors',
        toFloat64(count()),
        toFloat64(count())
    FROM ods.geo_by_click_errors
)
ORDER BY table_name
"""

SQL_TRUNCATE_DDS_CLICK = "TRUNCATE TABLE dds.click"
SQL_TRUNCATE_DDS_EVENT = "TRUNCATE TABLE dds.event"

SQL_CHECK_DDS_INTEGRITY = """
SELECT
    countIf(click_id IS NOT NULL AND click_id NOT IN (SELECT click_id FROM dds.click)) AS orphan_events
FROM dds.event
"""

SQL_VALIDATE_DM_SUMMARY = "SELECT count() AS dq_rows FROM dm.dq_summary"


# -----------------------------------------------------------------------------
# Управляющие функции
# -----------------------------------------------------------------------------
def assert_schema_ready(**context) -> None:
    """Падает, если DDL не применён полностью."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="precheck.check_schema_ready_sql")

    if not result or not result[0] or len(result[0]) != 4:
        raise AirflowException(f"Некорректный результат check_schema_ready_sql: {result}")

    if any(value == 0 for value in result[0]):
        raise AirflowException(
            "Схема не готова: сначала запустите DAG ddl_init, затем повторите etl_pipeline."
        )


def wait_for_stg_data(**context) -> None:
    """
    Ожидает появления строк в STG до заданного таймаута.
    Таймаут берётся из dag_run.conf.wait_stg_timeout_sec (или legacy wait_ods_timeout_sec)
    либо из params.
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    timeout_sec = int(
        conf.get(
            "wait_stg_timeout_sec",
            conf.get(
                "wait_ods_timeout_sec",
                context["params"]["wait_stg_timeout_sec"],
            ),
        )
    )
    poll_interval_sec = 10

    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default", database="default")
    started = time.monotonic()

    while True:
        rows = hook.execute(
            """
            SELECT
                (SELECT count() FROM stg.browser_raw)
                + (SELECT count() FROM stg.location_raw)
                + (SELECT count() FROM stg.device_raw)
                + (SELECT count() FROM stg.geo_raw) AS stg_rows_total
            """
        )
        count_rows = int(rows[0][0]) if rows else 0
        if count_rows > 0:
            return

        elapsed = int(time.monotonic() - started)
        if elapsed >= timeout_sec:
            raise AirflowException(
                f"Таймаут ожидания STG истёк ({timeout_sec} сек). "
                "Таблицы stg.*_raw всё ещё пусты."
            )

        time.sleep(poll_interval_sec)


def choose_full_refresh(**context) -> str:
    """Ветвление: делать TRUNCATE DDS или пропустить."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    full_refresh = parse_bool_param(
        conf.get("full_refresh", context["params"]["full_refresh"]),
        "full_refresh",
    )
    return "transform.truncate_dds_click" if full_refresh else "transform.skip_truncate"


def assert_dm_summary_not_empty(**context) -> None:
    """Проверяет, что dm.dq_summary заполнена после загрузки."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="transform.validate_dm_summary_sql")

    if not result or not result[0] or len(result[0]) != 1:
        raise AirflowException(f"Некорректный результат validate_dm_summary_sql: {result}")

    dq_rows = int(result[0][0])
    if dq_rows <= 0:
        raise AirflowException("dm.dq_summary пуста после load_dm_summary.")


def assert_dds_integrity(**context) -> None:
    """Падает, если события ссылаются на отсутствующие клики."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="transform.check_dds_integrity")

    if not result or not result[0] or len(result[0]) != 1:
        raise AirflowException(f"Некорректный результат check_dds_integrity: {result}")

    orphan_events = int(result[0][0])
    if orphan_events > 0:
        raise AirflowException(
            f"DDS integrity check failed: orphan_events={orphan_events}. "
            "Есть события, чей click_id отсутствует в dds.click."
        )


with DAG(
    dag_id="etl_pipeline",
    description="ETL STG -> ODS -> DDS -> DM для demo-проекта",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["etl", "clickhouse", "demo"],
    params={
        "full_refresh": Param(True, type="boolean"),
        "wait_stg_timeout_sec": Param(600, type="integer", minimum=30),
    },
) as dag:
    with TaskGroup(group_id="precheck") as precheck:
        check_clickhouse = ClickHouseOperator(
            task_id="check_clickhouse",
            sql=SQL_CHECK_CLICKHOUSE,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        check_schema_ready_sql = ClickHouseOperator(
            task_id="check_schema_ready_sql",
            sql=SQL_CHECK_SCHEMA_READY,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        check_schema_ready = PythonOperator(
            task_id="check_schema_ready",
            python_callable=assert_schema_ready,
        )

        check_clickhouse >> check_schema_ready_sql >> check_schema_ready

    with TaskGroup(group_id="transform") as transform:
        wait_for_stg_data_task = PythonOperator(
            task_id="wait_for_stg_data",
            python_callable=wait_for_stg_data,
        )

        load_ods = ClickHouseOperator(
            task_id="load_ods",
            sql=load_sql_statements("ods/20_stg_to_ods.sql"),
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        check_ods_quality = ClickHouseOperator(
            task_id="check_ods_quality",
            sql=SQL_CHECK_ODS_QUALITY,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        choose_refresh_mode = BranchPythonOperator(
            task_id="choose_refresh_mode",
            python_callable=choose_full_refresh,
        )

        truncate_dds_click = ClickHouseOperator(
            task_id="truncate_dds_click",
            sql=SQL_TRUNCATE_DDS_CLICK,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        truncate_dds_event = ClickHouseOperator(
            task_id="truncate_dds_event",
            sql=SQL_TRUNCATE_DDS_EVENT,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        skip_truncate = EmptyOperator(task_id="skip_truncate")

        truncate_complete = EmptyOperator(
            task_id="truncate_complete",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        load_dds = ClickHouseOperator(
            task_id="load_dds",
            sql=load_sql_statements("dds/30_ods_to_dds.sql"),
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        check_dds_integrity = ClickHouseOperator(
            task_id="check_dds_integrity",
            sql=SQL_CHECK_DDS_INTEGRITY,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        assert_dds_integrity_task = PythonOperator(
            task_id="assert_dds_integrity",
            python_callable=assert_dds_integrity,
            retries=0,
        )

        load_dm_summary = ClickHouseOperator(
            task_id="load_dm_summary",
            sql=load_sql_statements("dm/40_dds_to_dm.sql"),
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        validate_dm_summary_sql = ClickHouseOperator(
            task_id="validate_dm_summary_sql",
            sql=SQL_VALIDATE_DM_SUMMARY,
            clickhouse_conn_id="clickhouse_default",
            database="default",
        )

        validate_dm_summary = PythonOperator(
            task_id="validate_dm_summary",
            python_callable=assert_dm_summary_not_empty,
        )

        wait_for_stg_data_task >> load_ods >> check_ods_quality >> choose_refresh_mode
        choose_refresh_mode >> truncate_dds_click >> truncate_dds_event >> truncate_complete
        choose_refresh_mode >> skip_truncate >> truncate_complete
        (
            truncate_complete
            >> load_dds
            >> check_dds_integrity
            >> assert_dds_integrity_task
            >> load_dm_summary
            >> validate_dm_summary_sql
            >> validate_dm_summary
        )

    precheck >> transform
