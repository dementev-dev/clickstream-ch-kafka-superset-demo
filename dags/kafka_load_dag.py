"""
DAG для загрузки данных в Kafka из JSONL-файлов.

Функциональность:
- Проверка доступности Kafka и наличия файлов
- Создание/сброс топиков
- Параллельная загрузка 4 потоков данных
- Проверка результатов через XCom

Параметры (через Trigger DAG with config):
    limit (int): Количество строк для загрузки (по умолчанию 50)
    full_load (bool): Загрузить всё, игнорируя limit (по умолчанию False)
    reset_topics (bool): Пересоздать топики (по умолчанию True)
    load_browser (bool): Загружать browser_events (по умолчанию True)
    load_location (bool): Загружать location_events (по умолчанию True)
    load_device (bool): Загружать device_events (по умолчанию True)
    load_geo (bool): Загружать geo_events (по умолчанию True)
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
    get_topic_file_mapping,
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
    conf = context.get("dag_run", {}).conf or {}
    load_browser = bool(conf.get("load_browser", context["params"]["load_browser"]))
    load_location = bool(conf.get("load_location", context["params"]["load_location"]))
    load_device = bool(conf.get("load_device", context["params"]["load_device"]))
    load_geo = bool(conf.get("load_geo", context["params"]["load_geo"]))

    check_input_files(
        data_dir=DATA_DIR,
        load_browser=load_browser,
        load_location=load_location,
        load_device=load_device,
        load_geo=load_geo,
    )


def _validate_params(**context) -> None:
    """Валидация параметров загрузки."""
    conf = context.get("dag_run", {}).conf or {}
    limit = int(conf.get("limit", context["params"]["limit"]))
    load_browser = bool(conf.get("load_browser", context["params"]["load_browser"]))
    load_location = bool(conf.get("load_location", context["params"]["load_location"]))
    load_device = bool(conf.get("load_device", context["params"]["load_device"]))
    load_geo = bool(conf.get("load_geo", context["params"]["load_geo"]))

    validate_load_params(
        limit=limit,
        load_browser=load_browser,
        load_location=load_location,
        load_device=load_device,
        load_geo=load_geo,
    )


def _prepare_topics(**context) -> None:
    """Подготовка топиков Kafka (создание/сброс)."""
    conf = context.get("dag_run", {}).conf or {}
    reset_topics = bool(conf.get("reset_topics", context["params"]["reset_topics"]))
    load_browser = bool(conf.get("load_browser", context["params"]["load_browser"]))
    load_location = bool(conf.get("load_location", context["params"]["load_location"]))
    load_device = bool(conf.get("load_device", context["params"]["load_device"]))
    load_geo = bool(conf.get("load_geo", context["params"]["load_geo"]))

    # Получаем список топиков для загрузки
    mapping = get_topic_file_mapping(
        load_browser=load_browser,
        load_location=load_location,
        load_device=load_device,
        load_geo=load_geo,
    )
    topics = list(mapping.keys())

    prepare_topics(topics=topics, reset=reset_topics)


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
    conf = context.get("dag_run", {}).conf or {}
    load_browser = bool(conf.get("load_browser", context["params"]["load_browser"]))
    load_location = bool(conf.get("load_location", context["params"]["load_location"]))
    load_device = bool(conf.get("load_device", context["params"]["load_device"]))
    load_geo = bool(conf.get("load_geo", context["params"]["load_geo"]))

    ti = context["ti"]

    # Собираем результаты из XCom
    results = {}
    total_sent = 0

    if load_browser:
        count = ti.xcom_pull(task_ids="ingest.load_browser_events", key="browser_count")
        results["browser_events"] = count or 0
        total_sent += count or 0

    if load_location:
        count = ti.xcom_pull(task_ids="ingest.load_location_events", key="location_count")
        results["location_events"] = count or 0
        total_sent += count or 0

    if load_device:
        count = ti.xcom_pull(task_ids="ingest.load_device_events", key="device_count")
        results["device_events"] = count or 0
        total_sent += count or 0

    if load_geo:
        count = ti.xcom_pull(task_ids="ingest.load_geo_events", key="geo_count")
        results["geo_events"] = count or 0
        total_sent += count or 0

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
        "full_load": Param(
            default=False,
            type="boolean",
            description="Загрузить все данные, игнорируя limit",
        ),
        "reset_topics": Param(
            default=True,
            type="boolean",
            description="Пересоздать топики перед загрузкой",
        ),
        "load_browser": Param(
            default=True,
            type="boolean",
            description="Загружать browser_events",
        ),
        "load_location": Param(
            default=True,
            type="boolean",
            description="Загружать location_events",
        ),
        "load_device": Param(
            default=True,
            type="boolean",
            description="Загружать device_events",
        ),
        "load_geo": Param(
            default=True,
            type="boolean",
            description="Загружать geo_events",
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
