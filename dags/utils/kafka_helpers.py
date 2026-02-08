"""
Helper-функции для работы с Kafka из Airflow DAG'ов.

Использует kafka-python:
- KafkaAdminClient — для управления топиками
- KafkaProducer — для публикации сообщений
"""

from __future__ import annotations

import logging
from pathlib import Path

# -----------------------------------------------------------------------------
# Конфигурация подключения к Kafka
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
REQUEST_TIMEOUT_MS = 30000

# Топики и соответствующие файлы данных
TOPIC_FILE_MAP = {
    "browser_events": "browser_events.jsonl",
    "location_events": "location_events.jsonl",
    "device_events": "device_events.jsonl",
    "geo_events": "geo_events.jsonl",
}

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Проверка доступности Kafka
# -----------------------------------------------------------------------------
def check_kafka_ready(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    timeout_ms: int = REQUEST_TIMEOUT_MS,
) -> None:
    """
    Проверяет доступность Kafka брокера.

    Args:
        bootstrap_servers: Адрес Kafka брокера
        timeout_ms: Таймаут запроса в миллисекундах

    Raises:
        AirflowException: Если Kafka недоступна
    """
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
    from airflow.exceptions import AirflowException

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=timeout_ms,
        )
        # Проверяем связь, запрашивая список топиков
        admin_client.list_topics()
        admin_client.close()
        logger.info("Kafka брокер доступен: %s", bootstrap_servers)
    except NoBrokersAvailable as e:
        raise AirflowException(f"Kafka брокер недоступен: {bootstrap_servers}") from e
    except Exception as e:
        raise AirflowException(f"Ошибка подключения к Kafka: {e}") from e


# -----------------------------------------------------------------------------
# Управление топиками
# -----------------------------------------------------------------------------
def prepare_topics(
    topics: list[str] | None = None,
    reset: bool = True,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    timeout_ms: int = REQUEST_TIMEOUT_MS,
) -> None:
    """
    Создаёт или пересоздаёт топики Kafka.

    Args:
        topics: Список топиков для создания (по умолчанию все из TOPIC_FILE_MAP)
        reset: Если True — удаляет топики перед созданием
        bootstrap_servers: Адрес Kafka брокера
        timeout_ms: Таймаут операций в миллисекундах
    """
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
    from airflow.exceptions import AirflowException

    if topics is None:
        topics = list(TOPIC_FILE_MAP.keys())

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=timeout_ms,
    )

    try:
        # Удаляем топики если reset=True
        if reset:
            try:
                admin_client.delete_topics(topics, timeout_ms=timeout_ms)
                logger.info("Удалены топики: %s", topics)
            except UnknownTopicOrPartitionError:
                # Топики не существуют — это нормально
                logger.info("Топики для удаления не найдены (уже отсутствуют)")
            except Exception as e:
                logger.warning("Ошибка при удалении топиков: %s", e)

        # Создаём топики
        new_topics = [
            NewTopic(
                name=topic,
                num_partitions=1,  # Дефолтное количество партиций
                replication_factor=1,
            )
            for topic in topics
        ]

        try:
            admin_client.create_topics(new_topics, timeout_ms=timeout_ms)
            logger.info("Созданы топики: %s", topics)
        except TopicAlreadyExistsError:
            logger.info("Топики уже существуют: %s", topics)
        except Exception as e:
            raise AirflowException(f"Ошибка создания топиков: {e}") from e

    finally:
        admin_client.close()


# -----------------------------------------------------------------------------
# Загрузка данных из JSONL
# -----------------------------------------------------------------------------
def load_jsonl(
    file_path: str | Path,
    topic: str,
    limit: int = 0,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> int:
    """
    Читает JSONL-файл и публикует строки в Kafka топик.

    Формат: 1 строка JSON = 1 сообщение (value), без ключа.

    Args:
        file_path: Путь к .jsonl файлу
        topic: Имя Kafka топика
        limit: Максимальное количество строк (0 = все строки)
        bootstrap_servers: Адрес Kafka брокера

    Returns:
        Количество отправленных сообщений

    Raises:
        AirflowException: Если файл не найден или ошибка отправки
    """
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    from airflow.exceptions import AirflowException

    file_path = Path(file_path)
    if not file_path.is_file():
        raise AirflowException(f"Файл не найден: {file_path}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        # Отправляем сырые байты (строки JSON как есть)
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        acks="all",  # Ждём подтверждения от всех реплик
        retries=3,
        batch_size=16384,
        linger_ms=10,
    )

    sent_count = 0
    error_count = 0

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                # Пропускаем пустые строки
                line = line.strip()
                if not line:
                    continue

                # Проверяем лимит
                if limit > 0 and sent_count >= limit:
                    logger.info(
                        "Достигнут лимит %d строк для %s", limit, topic
                    )
                    break

                # Отправляем сообщение
                try:
                    future = producer.send(topic, value=line)
                    # Неблокирующая отправка, собираем future для проверки
                    sent_count += 1
                except KafkaError as e:
                    error_count += 1
                    logger.error("Ошибка отправки строки %d в %s: %s", line_num, topic, e)
                    if error_count > 10:
                        raise AirflowException(
                            f"Слишком много ошибок отправки в {topic}"
                        ) from e

        # Ждём завершения всех отправок
        producer.flush(timeout=60)

        logger.info(
            "Загрузка завершена: %s -> %s, отправлено %d сообщений",
            file_path.name,
            topic,
            sent_count,
        )

        if sent_count == 0:
            raise AirflowException(f"Не отправлено ни одного сообщения в {topic}")

        return sent_count

    except Exception as e:
        if isinstance(e, AirflowException):
            raise
        raise AirflowException(f"Ошибка загрузки {file_path.name}: {e}") from e

    finally:
        producer.close(timeout=30)


# -----------------------------------------------------------------------------
# Утилиты для валидации
# -----------------------------------------------------------------------------
def validate_load_params(
    limit: int,
) -> None:
    """
    Валидирует параметры загрузки данных.

    Args:
        limit: Количество строк для загрузки

    Raises:
        AirflowException: Если параметры невалидны
    """
    from airflow.exceptions import AirflowException

    # Проверка limit
    if not isinstance(limit, int) or limit < 0:
        raise AirflowException(f"limit должен быть неотрицательным int, получено: {limit}")


def check_input_files(
    data_dir: str | Path = "/opt/airflow/data",
) -> None:
    """
    Проверяет наличие необходимых JSONL-файлов.

    Args:
        data_dir: Директория с данными

    Raises:
        AirflowException: Если какой-либо файл отсутствует
    """
    from airflow.exceptions import AirflowException

    data_dir = Path(data_dir)
    files_to_check = list(TOPIC_FILE_MAP.values())

    missing_files = []
    for filename in files_to_check:
        file_path = data_dir / filename
        if not file_path.is_file():
            missing_files.append(filename)

    if missing_files:
        raise AirflowException(
            f"Отсутствуют файлы данных в {data_dir}: {missing_files}"
        )

    logger.info("Все необходимые файлы найдены: %s", files_to_check)
