"""
DAG для загрузки данных в Kafka из JSONL-файлов.

Функциональность:
- Проверка доступности Kafka и наличия файлов
- Создание/сброс топиков
- Параллельная загрузка 4 потоков данных
- Проверка результатов через XCom

Параметры (через Trigger DAG with config):
    limit (int): Количество строк для загрузки (по умолчанию 0 = все)
    reset_topics (bool): Пересоздать топики (по умолчанию True)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Импортируем helper-функции
from utils.kafka_helpers import (
    check_input_files,
    check_kafka_ready,
    load_jsonl,
    prepare_topics,
    validate_load_params,
)

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

DATA_DIR = Path("/opt/airflow/data")

# -----------------------------------------------------------------------------
# Python callable функции для задач
# -----------------------------------------------------------------------------
def _check_kafka(**context) -> None:
    """Проверка доступности Kafka брокера."""
    check_kafka_ready()


def _check_files(**context) -> None:
    """Проверка наличия входных файлов."""
    check_input_files(data_dir=DATA_DIR)


def _validate_params(**context) -> None:
    """Валидация параметров загрузки."""
    conf = context.get("dag_run", {}).conf or {}
    limit = int(conf.get("limit", context["params"]["limit"]))
    validate_load_params(limit=limit)


def _prepare_topics(**context) -> None:
    """Подготовка топиков Kafka (создание/сброс)."""
    conf = context.get("dag_run", {}).conf or {}
    reset_topics = bool(conf.get("reset_topics", context["params"]["reset_topics"]))

    prepare_topics(reset=reset_topics)


def _load_events(event_type: str, **context) -> int:
    """
    Загружает события определённого типа в Kafka.

    Args:
        event_type: Тип события (browser, location, device, geo)

    Returns:
        Количество отправленных сообщений
    """
    conf = context.get("dag_run", {}).conf or {}
    limit = int(conf.get("limit", context["params"]["limit"]))

    # Маппинг типа события на топик и файл
    event_mapping = {
        "browser": ("browser_events", "browser_events.jsonl"),
        "location": ("location_events", "location_events.jsonl"),
        "device": ("device_events", "device_events.jsonl"),
        "geo": ("geo_events", "geo_events.jsonl"),
    }

    topic, filename = event_mapping[event_type]
    file_path = DATA_DIR / filename

    # Загружаем данные
    sent_count = load_jsonl(
        file_path=file_path,
        topic=topic,
        limit=limit,
    )

    # Сохраняем результат в XCom для verify_publish_counts
    context["ti"].xcom_push(key=f"{event_type}_count", value=sent_count)

    return sent_count


def _verify_counts(**context) -> None:
    """Проверяет, что все загрузки отправили сообщения."""
    ti = context["ti"]

    # Собираем результаты из XCom
    results = {
        "browser_events": ti.xcom_pull(task_ids="ingest.load_browser_events", key="browser_count") or 0,
        "location_events": ti.xcom_pull(task_ids="ingest.load_location_events", key="location_count") or 0,
        "device_events": ti.xcom_pull(task_ids="ingest.load_device_events", key="device_count") or 0,
        "geo_events": ti.xcom_pull(task_ids="ingest.load_geo_events", key="geo_count") or 0,
    }
    total_sent = sum(results.values())

    # Проверяем, что отправлено хотя бы что-то
    if total_sent == 0:
        raise AirflowException("Не отправлено ни одного сообщения ни в один топик")

    # Логируем итоговую статистику
    for topic, count in results.items():
        print(f"✓ {topic}: {count} сообщений")
    print(f"\nВсего отправлено: {total_sent} сообщений")


# -----------------------------------------------------------------------------
# Определение DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="kafka_load",
    default_args=default_args,
    description="Загрузка данных из JSONL в Kafka топики",
    schedule=None,  # Только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["kafka", "ingest", "experiments"],
    params={
        "limit": Param(
            default=0,
            type="integer",
            description="Количество строк для загрузки (0 = все строки, по умолчанию)",
        ),
        "reset_topics": Param(
            default=True,
            type="boolean",
            description="Пересоздать топики перед загрузкой",
        ),
    },
) as dag:

    # -------------------------------------------------------------------------
    # TaskGroup: precheck — проверки перед загрузкой
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="precheck") as precheck:
        check_kafka = PythonOperator(
            task_id="check_kafka",
            python_callable=_check_kafka,
        )

        check_input_files_task = PythonOperator(
            task_id="check_input_files",
            python_callable=_check_files,
        )

        validate_params = PythonOperator(
            task_id="validate_load_params",
            python_callable=_validate_params,
        )

        check_kafka >> check_input_files_task >> validate_params

    # -------------------------------------------------------------------------
    # TaskGroup: ingest — загрузка данных
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="ingest") as ingest:
        prepare_topics_task = PythonOperator(
            task_id="prepare_topics",
            python_callable=_prepare_topics,
        )

        load_browser_events = PythonOperator(
            task_id="load_browser_events",
            python_callable=_load_events,
            op_kwargs={"event_type": "browser"},
        )

        load_location_events = PythonOperator(
            task_id="load_location_events",
            python_callable=_load_events,
            op_kwargs={"event_type": "location"},
        )

        load_device_events = PythonOperator(
            task_id="load_device_events",
            python_callable=_load_events,
            op_kwargs={"event_type": "device"},
        )

        load_geo_events = PythonOperator(
            task_id="load_geo_events",
            python_callable=_load_events,
            op_kwargs={"event_type": "geo"},
        )

        verify_publish_counts = PythonOperator(
            task_id="verify_publish_counts",
            python_callable=_verify_counts,
        )

        # Зависимости: подготовка -> параллельная загрузка -> проверка
        prepare_topics_task >> [
            load_browser_events,
            load_location_events,
            load_device_events,
            load_geo_events,
        ] >> verify_publish_counts

    # -------------------------------------------------------------------------
    # Итоговая цепочка
    # -------------------------------------------------------------------------
    precheck >> ingest
