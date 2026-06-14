"""Kafka-интеграция генератора."""

import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime

from clickstream_generator.metrics import METRICS_ERRORS_TOTAL, METRICS_EVENTS_TOTAL
from clickstream_generator.state import GeneratorState


logger = logging.getLogger("generator")

_kafka_imported = False
KafkaProducer = None
KafkaError = None


def _import_kafka():
    """Лениво импортирует Kafka-клиент, чтобы тесты могли работать без Kafka."""
    global _kafka_imported, KafkaProducer, KafkaError
    if not _kafka_imported:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        _kafka_imported = True
    return KafkaProducer, KafkaError


def _with_retry(operation, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0):
    """Выполняет операцию с экспоненциальным backoff."""
    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                logger.warning(
                    f"Operation failed (attempt {attempt + 1}/{max_retries}): "
                    f"{e}. Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"Operation failed after {max_retries} attempts: {e}")
                raise last_exception


def _facade_attr(name: str, fallback):
    """Берёт совместимый mock из фасада generator, если тест его подменил."""
    facade = sys.modules.get("generator")
    return getattr(facade, name, fallback) if facade else fallback


def _retry(operation, **kwargs):
    return _facade_attr("_with_retry", _with_retry)(operation, **kwargs)


def _kafka_importer():
    return _facade_attr("_import_kafka", _import_kafka)


@dataclass
class BatchRecord:
    """Запись об отправленном батче."""

    batch_id: str
    started_at: datetime
    finished_at: datetime
    sent_total: int
    sent_browser: int
    sent_location: int
    sent_device: int
    sent_geo: int
    status: str
    error_message: str | None = None

    def to_dict(self) -> dict:
        """Конвертирует в словарь для сериализации."""
        return {
            "batch_id": self.batch_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "sent_total": self.sent_total,
            "sent_browser": self.sent_browser,
            "sent_location": self.sent_location,
            "sent_device": self.sent_device,
            "sent_geo": self.sent_geo,
            "status": self.status,
            "error_message": self.error_message,
        }


class KafkaStateManager:
    """Управление состоянием генератора в Kafka compact topic."""

    STATE_TOPIC = "generator_state"
    STATE_KEY = "default"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        KafkaProducerCls, _ = _kafka_importer()()

        logger.info(f"Connecting to Kafka for state management at {self.bootstrap_servers}")
        self.producer = KafkaProducerCls(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
            retry_backoff_ms=1000,
        )
        logger.info("Connected to Kafka for state management successfully")

    def save(self, state: GeneratorState) -> None:
        """Сохраняет состояние в топик."""
        value = state.to_dict()

        def _do_send():
            self.producer.send(self.STATE_TOPIC, key=self.STATE_KEY, value=value)

        _retry(_do_send, max_retries=3, base_delay=0.5)

    def flush(self) -> None:
        """Сбрасывает буфер с retry."""
        def _do_flush():
            self.producer.flush()

        _retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self) -> None:
        """Закрывает соединение."""
        try:
            self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing producer (ignored): {e}")

    def load(self) -> GeneratorState | None:
        """Загружает последнее состояние из топика."""
        from kafka import KafkaConsumer

        logger.info(f"Loading state from topic {self.STATE_TOPIC}")

        def _do_load():
            consumer = KafkaConsumer(
                self.STATE_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )

            last_state = None
            for message in consumer:
                if message.key and message.key.decode("utf-8") == self.STATE_KEY:
                    last_state = message.value

            consumer.close()
            return last_state

        try:
            last_state = _retry(_do_load, max_retries=3, base_delay=0.5)

            if last_state:
                logger.info(
                    f"Restored state: tick={last_state.get('tick')}, "
                    f"last_batch_id={last_state.get('last_batch_id')}"
                )
                restored = GeneratorState.from_dict_safe(last_state)
                if restored is None:
                    logger.warning("State data was invalid, starting fresh")
                return restored

            logger.info("No previous state found, starting fresh")
            return None

        except Exception as e:
            logger.warning(f"Failed to load state: {e}, starting fresh")
            return None


class KafkaStartupHistoryManifest:
    """Хранение манифеста стартовой истории в Kafka compact topic."""

    MANIFEST_TOPIC = "generator_startup_history_manifest"
    MANIFEST_KEY = "default"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        KafkaProducerCls, _ = _kafka_importer()()

        logger.info(
            f"Connecting to Kafka for startup history manifest at {self.bootstrap_servers}"
        )
        self.producer = KafkaProducerCls(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
            retry_backoff_ms=1000,
        )
        logger.info("Connected to Kafka for startup history manifest successfully")

    def save(self, manifest: dict) -> None:
        """Сохраняет манифест стартовой истории."""
        def _do_send():
            self.producer.send(
                self.MANIFEST_TOPIC,
                key=self.MANIFEST_KEY,
                value=manifest,
            )

        _retry(_do_send, max_retries=3, base_delay=0.5)

    def flush(self) -> None:
        """Сбрасывает буфер с retry."""
        def _do_flush():
            self.producer.flush()

        _retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self) -> None:
        """Закрывает соединение."""
        try:
            self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing manifest producer (ignored): {e}")

    def load(self) -> dict | None:
        """Загружает последний манифест стартовой истории."""
        from kafka import KafkaConsumer

        logger.info(f"Loading startup history manifest from topic {self.MANIFEST_TOPIC}")

        def _do_load():
            consumer = KafkaConsumer(
                self.MANIFEST_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )

            last_manifest = None
            for message in consumer:
                if message.key and message.key.decode("utf-8") == self.MANIFEST_KEY:
                    last_manifest = message.value

            consumer.close()
            return last_manifest

        try:
            return _retry(_do_load, max_retries=3, base_delay=0.5)
        except Exception as e:
            logger.warning(f"Failed to load startup history manifest: {e}")
            return None


def ensure_topics(bootstrap_servers: str) -> None:
    """Создаёт служебные топики, если их ещё нет."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    def _create_topics():
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            history_topic = NewTopic(
                name=KafkaBatchHistory.HISTORY_TOPIC,
                num_partitions=1,
                replication_factor=1,
            )
            state_topic = NewTopic(
                name=KafkaStateManager.STATE_TOPIC,
                num_partitions=1,
                replication_factor=1,
                topic_configs={
                    "cleanup.policy": "compact",
                    "min.cleanable.dirty.ratio": "0.1",
                    "delete.retention.ms": "100",
                },
            )
            manifest_topic = NewTopic(
                name=KafkaStartupHistoryManifest.MANIFEST_TOPIC,
                num_partitions=1,
                replication_factor=1,
                topic_configs={
                    "cleanup.policy": "compact",
                    "min.cleanable.dirty.ratio": "0.1",
                    "delete.retention.ms": "100",
                },
            )

            for topic in [history_topic, state_topic, manifest_topic]:
                try:
                    admin_client.create_topics([topic])
                    logger.info(f"Created topic: {topic.name}")
                except TopicAlreadyExistsError:
                    logger.debug(f"Topic already exists: {topic.name}")
        finally:
            admin_client.close()

    _retry(_create_topics, max_retries=5, base_delay=1.0)


class KafkaBatchHistory:
    """Хранение истории batch в Kafka."""

    HISTORY_TOPIC = "generator_batch_history"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka с retry."""
        KafkaProducerCls, _ = _kafka_importer()()

        def _do_connect():
            logger.info(f"Connecting to Kafka for history at {self.bootstrap_servers}")
            self.producer = KafkaProducerCls(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
                retry_backoff_ms=1000,
            )
            logger.info("Connected to Kafka for history successfully")

        _retry(_do_connect, max_retries=5, base_delay=1.0)

    def add(self, record: BatchRecord):
        """Добавляет запись в историю."""
        key = record.batch_id
        value = record.to_dict()

        def _do_send():
            self.producer.send(self.HISTORY_TOPIC, key=key, value=value)

        try:
            _retry(_do_send, max_retries=3, base_delay=0.5)
        except Exception as e:
            logger.warning(f"Failed to send history record after retries: {e}, attempting reconnect")
            self._connect()
            _retry(_do_send, max_retries=2, base_delay=0.5)

    def flush(self):
        """Сбрасывает буфер с retry."""
        def _do_flush():
            self.producer.flush()

        _retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self):
        """Закрывает соединение."""
        try:
            if self.producer:
                self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing history producer (ignored): {e}")


class KafkaPublisher:
    """Публикация событий в Kafka с retry и реконнектом."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka с retry."""
        KafkaProducerCls, _ = _kafka_importer()()

        def _do_connect():
            logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
            self.producer = KafkaProducerCls(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                batch_size=16384,
                linger_ms=100,
                retries=3,
                retry_backoff_ms=1000,
            )
            logger.info("Connected to Kafka successfully")

        _retry(_do_connect, max_retries=5, base_delay=1.0)

    def _publish_with_retry(self, topic: str, events: list[dict]) -> tuple[int, int]:
        """Внутренняя функция публикации с retry на уровне batch."""
        if not self.producer:
            raise RuntimeError("Producer not connected")

        sent = 0
        errors = 0
        futures = []

        for event in events:
            key = event.get("event_id") or event.get("click_id")
            try:
                future = self.producer.send(topic, key=key, value=event)
                futures.append(future)
            except Exception as e:
                logger.error(f"Failed to send message to {topic}: {e}")
                errors += 1
                METRICS_ERRORS_TOTAL.labels(topic=topic).inc()

        for future in futures:
            try:
                future.get(timeout=10)
                sent += 1
                METRICS_EVENTS_TOTAL.labels(topic=topic).inc()
            except Exception as e:
                logger.error(f"Failed to confirm message delivery: {e}")
                errors += 1
                METRICS_ERRORS_TOTAL.labels(topic=topic).inc()

        return sent, errors

    def publish(self, topic: str, events: list[dict]) -> tuple[int, int]:
        """Публикует события в топик с retry и автоматическим реконнектом."""
        def _do_publish():
            return self._publish_with_retry(topic, events)

        try:
            return _retry(_do_publish, max_retries=3, base_delay=0.5)
        except Exception as e:
            logger.warning(f"Publish failed after retries: {e}, attempting reconnect")
            self._connect()
            return _retry(_do_publish, max_retries=2, base_delay=0.5)

    def flush(self):
        """Сбрасывает буфер с retry."""
        def _do_flush():
            if self.producer:
                self.producer.flush()

        _retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self):
        """Закрывает соединение."""
        try:
            if self.producer:
                self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing producer (ignored): {e}")
