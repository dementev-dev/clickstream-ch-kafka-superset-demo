#!/usr/bin/env python3
"""
Автономный генератор событий для Kafka (MVP rev5).

Режим 'steady-stream': публикуем постепенно, короткими тиками (1-10 сек),
держим целевую интенсивность events/min без крупных минутных batch.
"""

import json
import logging
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Prometheus метрики
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Kafka импортируем lazy для возможности тестирования без Kafka
_kafka_imported = False
KafkaProducer = None
KafkaError = None


def _import_kafka():
    global _kafka_imported, KafkaProducer, KafkaError
    if not _kafka_imported:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        _kafka_imported = True
    return KafkaProducer, KafkaError


# ---------------------------------------------------------------------------
# Настройка логирования
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("generator")


# ---------------------------------------------------------------------------
# Prometheus метрики
# ---------------------------------------------------------------------------
METRICS_EVENTS_TOTAL = Counter(
    "generator_events_total",
    "Total number of events sent to Kafka",
    ["topic"]
)
METRICS_ERRORS_TOTAL = Counter(
    "generator_publish_errors_total",
    "Total number of publish errors",
    ["topic"]
)
METRICS_TICK_DURATION = Histogram(
    "generator_tick_duration_seconds",
    "Duration of generator tick in seconds"
)
METRICS_LAST_SUCCESS = Gauge(
    "generator_last_success_timestamp",
    "Unix timestamp of last successful tick"
)


# ---------------------------------------------------------------------------
# Конфигурация через env
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    """Конфигурация генератора из переменных окружения."""

    # Подключение к Kafka
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )

    # Параметры генерации
    tick_seconds: int = field(
        default_factory=lambda: int(os.getenv("GEN_TICK_SECONDS", "5"))
    )
    lambda_base_per_min: int = field(
        default_factory=lambda: int(os.getenv("GEN_LAMBDA_BASE_PER_MIN", "200"))
    )
    jitter_pct: int = field(
        default_factory=lambda: int(os.getenv("GEN_JITTER_PCT", "20"))
    )
    min_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MIN_EVENTS_PER_TICK", "5"))
    )
    max_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MAX_EVENTS_PER_TICK", "50"))
    )

    # Пути к данным
    data_dir: Path = field(
        default_factory=lambda: Path(os.getenv("GEN_DATA_DIR", "/data"))
    )

    # Сид для воспроизводимости
    seed: int | None = field(
        default_factory=lambda: int(os.getenv("GEN_SEED"))
        if os.getenv("GEN_SEED")
        else None
    )

    # Включение/выключение генерации
    enabled: bool = field(
        default_factory=lambda: os.getenv("GEN_ENABLED", "true").lower() == "true"
    )

    # Порт для Prometheus метрик
    metrics_port: int = field(
        default_factory=lambda: int(os.getenv("GEN_METRICS_PORT", "9109"))
    )

    # Управление сохранением состояния
    state_enabled: bool = field(
        default_factory=lambda: os.getenv("GEN_STATE_ENABLED", "true").lower() == "true"
    )
    state_reset: bool = field(
        default_factory=lambda: os.getenv("GEN_STATE_RESET", "false").lower() == "true"
    )

    def __post_init__(self):
        # Валидация параметров
        if self.tick_seconds < 1:
            raise ValueError("GEN_TICK_SECONDS must be >= 1")
        if self.lambda_base_per_min < 1:
            raise ValueError("GEN_LAMBDA_BASE_PER_MIN must be >= 1")
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {self.data_dir}")


# ---------------------------------------------------------------------------
# Загрузка базового словаря событий
# ---------------------------------------------------------------------------
@dataclass
class EventDictionary:
    """Базовый словарь событий из JSONL файлов."""

    browser_events: list[dict[str, Any]]
    location_events: list[dict[str, Any]]
    device_events: list[dict[str, Any]]
    geo_events: list[dict[str, Any]]

    # Индексы для быстрого поиска
    browser_by_click_id: dict[str, list[dict]] = field(default_factory=dict)
    location_by_event_id: dict[str, dict] = field(default_factory=dict)
    device_by_click_id: dict[str, dict] = field(default_factory=dict)
    geo_by_click_id: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self):
        # Строим индексы для связности
        for browser in self.browser_events:
            self.browser_by_click_id.setdefault(browser["click_id"], []).append(browser)
        for loc in self.location_events:
            self.location_by_event_id[loc["event_id"]] = loc
        for dev in self.device_events:
            self.device_by_click_id[dev["click_id"]] = dev
        for geo in self.geo_events:
            self.geo_by_click_id[geo["click_id"]] = geo

    @classmethod
    def load(cls, data_dir: Path) -> "EventDictionary":
        """Загружает события из JSONL файлов."""
        logger.info(f"Loading event dictionary from {data_dir}")

        def load_jsonl(filename: str) -> list[dict]:
            path = data_dir / filename
            events = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
            logger.info(f"  Loaded {len(events)} events from {filename}")
            return events

        browser_events = load_jsonl("browser_events.jsonl")
        location_events = load_jsonl("location_events.jsonl")
        device_events = load_jsonl("device_events.jsonl")
        geo_events = load_jsonl("geo_events.jsonl")

        if not browser_events:
            raise ValueError("browser_events.jsonl is empty or missing")

        return cls(
            browser_events=browser_events,
            location_events=location_events,
            device_events=device_events,
            geo_events=geo_events,
        )


# ---------------------------------------------------------------------------
# Генерация событий
# ---------------------------------------------------------------------------
class EventGenerator:
    """Генератор событий с сохранением связности."""

    def __init__(self, dictionary: EventDictionary, config: Config):
        self.dictionary = dictionary
        self.config = config
        self.rng = random.Random(config.seed)

    def _new_uuid(self) -> str:
        """Генерирует новый UUID."""
        return str(uuid.uuid4())

    def _current_timestamp(self) -> str:
        """Возвращает текущую метку времени в формате JSONL."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    def _hour_factor(self) -> float:
        """Возвращает коэффициент интенсивности в зависимости от часа дня."""
        hour = datetime.now(timezone.utc).hour
        # Дневное окно (9-18): 1.2
        # Ночное окно (0-5): 0.7
        # Остальное время: 1.0
        if 9 <= hour <= 18:
            return 1.2
        elif 0 <= hour <= 5:
            return 0.7
        return 1.0

    def _calculate_events_count(self) -> int:
        """Вычисляет количество событий для текущего тика (Poisson + jitter)."""
        # Базовая интенсивность с учётом часа
        lambda_minute = self.config.lambda_base_per_min * self._hour_factor()

        # Масштабируем на длительность тика
        lambda_tick = lambda_minute * (self.config.tick_seconds / 60.0)

        # Генерируем Poisson
        count = 0
        L = math.exp(-lambda_tick)
        p = 1.0
        while p > L:
            p *= self.rng.random()
            count += 1
        count -= 1

        # Применяем jitter (вариативность)
        if self.config.jitter_pct > 0:
            jitter_factor = 1.0 + self.rng.uniform(
                -self.config.jitter_pct / 100.0,
                self.config.jitter_pct / 100.0
            )
            count = int(count * jitter_factor)

        # Применяем границы
        count = max(self.config.min_events_per_tick, min(count, self.config.max_events_per_tick))

        return count

    def generate_batch(self, batch_size: int) -> dict[str, list[dict]]:
        """
        Генерирует батч событий с сохранением связей.

        Возвращает словарь {topic: [events]}
        """
        if not self.dictionary.browser_events:
            logger.warning("Event dictionary is empty, skipping batch generation")
            return {
                "browser_events": [],
                "location_events": [],
                "device_events": [],
                "geo_events": [],
            }

        batch = {
            "browser_events": [],
            "location_events": [],
            "device_events": [],
            "geo_events": [],
        }

        if batch_size <= 0:
            return batch

        visit_candidates = [
            click_id for click_id, browser_events in self.dictionary.browser_by_click_id.items()
            if (
                len(browser_events) >= batch_size
                and click_id in self.dictionary.device_by_click_id
                and click_id in self.dictionary.geo_by_click_id
                and all(
                    event["event_id"] in self.dictionary.location_by_event_id
                    for event in browser_events[:batch_size]
                )
            )
        ]
        if visit_candidates:
            base_click_id = self.rng.choice(visit_candidates)
            base_browser_events = self.dictionary.browser_by_click_id[base_click_id][:batch_size]
        else:
            # Крайний случай для очень малого сида: сохраняем форму визита,
            # даже если приходится брать события с повторением.
            base_browser = self.rng.choice(self.dictionary.browser_events)
            base_click_id = base_browser["click_id"]
            base_browser_events = [base_browser for _ in range(batch_size)]

        base_device = self.dictionary.device_by_click_id.get(base_click_id)
        base_geo = self.dictionary.geo_by_click_id.get(base_click_id)
        new_click_id = self._new_uuid()

        for base_browser in base_browser_events:
            base_location = self.dictionary.location_by_event_id.get(base_browser["event_id"])

            # Генерируем новые ID
            new_event_id = self._new_uuid()
            new_timestamp = self._current_timestamp()

            # Создаём новое браузерное событие
            browser_event = {
                **base_browser,
                "event_id": new_event_id,
                "click_id": new_click_id,
                "event_timestamp": new_timestamp,
            }
            batch["browser_events"].append(browser_event)

            # Связанное location событие
            if base_location:
                location_event = {
                    **base_location,
                    "event_id": new_event_id,
                }
                batch["location_events"].append(location_event)

            # Связанное device событие
            if base_device:
                device_event = {
                    **base_device,
                    "click_id": new_click_id,
                }
                batch["device_events"].append(device_event)

            # Связанное geo событие
            if base_geo:
                geo_event = {
                    **base_geo,
                    "click_id": new_click_id,
                }
                batch["geo_events"].append(geo_event)

        return batch


# ---------------------------------------------------------------------------
# Batch record для истории
# ---------------------------------------------------------------------------
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
    status: str  # 'success', 'partial', 'error'
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


# ---------------------------------------------------------------------------
# State record для сохранения состояния генератора
# ---------------------------------------------------------------------------
def _nested_list_to_tuple(obj):
    """Рекурсивно преобразует list в tuple (для восстановления RNG state после JSON)."""
    if isinstance(obj, list):
        return tuple(_nested_list_to_tuple(x) for x in obj)
    return obj


@dataclass
class GeneratorState:
    """Состояние генератора для восстановления после рестарта.

    Используем JSON-safe сериализацию:
    - rng_state от random.getstate() - кортеж из простых типов (int, tuple),
      безопасно сериализуется в JSON напрямую без pickle
    """

    tick: int
    rng_state: tuple  # результат random.getstate() - JSON-serializable
    last_batch_id: str
    last_timestamp: datetime
    version: str = "1.0"

    def to_dict(self) -> dict:
        """Конвертирует в словарь для JSON-сериализации."""
        return {
            "tick": self.tick,
            "rng_state": self.rng_state,  # tuple из int - JSON-serializable
            "last_batch_id": self.last_batch_id,
            "last_timestamp": self.last_timestamp.isoformat(),
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GeneratorState":
        """Создаёт состояние из словаря (JSON-only, без pickle).
        
        При невалидном rng_state логирует предупреждение и возвращает None
        (вызывающий код должен обработать как "начать с чистого листа").
        """
        try:
            rng_state_raw = data.get("rng_state")
            if not rng_state_raw:
                logger.warning("State missing rng_state field")
                raise ValueError("rng_state is missing")
            
            rng_state = _nested_list_to_tuple(rng_state_raw)
            
            # Валидация: rng_state должен быть tuple и иметь минимальную структуру
            if not isinstance(rng_state, tuple):
                logger.warning(f"rng_state is not tuple: {type(rng_state)}")
                raise ValueError("rng_state must be tuple")
            
            if len(rng_state) < 2:
                logger.warning(f"rng_state has insufficient length: {len(rng_state)}")
                raise ValueError("rng_state has insufficient length")
            
            # Проверка что можем создать RNG и вызвать setstate (тестовая валидация)
            test_rng = random.Random()
            test_rng.setstate(rng_state)
            
            return cls(
                tick=data.get("tick", 0),
                rng_state=rng_state,
                last_batch_id=data.get("last_batch_id", ""),
                last_timestamp=datetime.fromisoformat(data.get("last_timestamp", "1970-01-01T00:00:00+00:00")),
                version=data.get("version", "1.0"),
            )
        except Exception as e:
            logger.warning(f"Invalid state format, will start fresh: {e}")
            raise ValueError(f"Invalid state: {e}")

    @classmethod
    def from_dict_safe(cls, data: dict) -> "GeneratorState | None":
        """Безопасная загрузка state с graceful degradation.
        
        Returns:
            GeneratorState если данные валидны, иначе None (начать с чистого листа).
        """
        try:
            return cls.from_dict(data)
        except Exception:
            # Уже залогировано в from_dict
            return None


# ---------------------------------------------------------------------------
# Kafka state manager - сохраняет/восстанавливает состояние генератора
# ---------------------------------------------------------------------------
class KafkaStateManager:
    """Управление состоянием генератора в Kafka (compact topic)."""

    STATE_TOPIC = "generator_state"
    STATE_KEY = "default"  # Для возможности нескольких генераторов в будущем

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        KafkaProducerCls, _ = _import_kafka()

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
        """Сохраняет состояние в топик (compact topic - только последнее значение).
        
        Использует retry при сбоях подключения к Kafka.
        """
        value = state.to_dict()
        
        def _do_send():
            self.producer.send(self.STATE_TOPIC, key=self.STATE_KEY, value=value)
        
        _with_retry(_do_send, max_retries=3, base_delay=0.5)

    def flush(self) -> None:
        """Сбрасывает буфер с retry."""
        def _do_flush():
            self.producer.flush()
        
        _with_retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self) -> None:
        """Закрывает соединение."""
        try:
            self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing producer (ignored): {e}")

    def load(self) -> GeneratorState | None:
        """Загружает последнее состояние из топика.

        Для compact topic хранится только последнее значение для ключа,
        поэтому читаем все сообщения и берём последнее с нужным ключом.
        Использует retry при сбоях подключения к Kafka.
        """
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
            last_state = _with_retry(_do_load, max_retries=3, base_delay=0.5)

            if last_state:
                logger.info(f"Restored state: tick={last_state.get('tick')}, "
                           f"last_batch_id={last_state.get('last_batch_id')}")
                # Используем from_dict_safe для graceful degradation при битом state
                restored = GeneratorState.from_dict_safe(last_state)
                if restored is None:
                    logger.warning("State data was invalid, starting fresh")
                return restored
            else:
                logger.info("No previous state found, starting fresh")
                return None

        except Exception as e:
            logger.warning(f"Failed to load state: {e}, starting fresh")
            return None


# ---------------------------------------------------------------------------
# Утилиты для работы с Kafka с retry/backoff
# ---------------------------------------------------------------------------
def _with_retry(operation, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0):
    """Выполняет операцию с экспоненциальным backoff и ограниченным числом попыток.

    Args:
        operation: функция для выполнения
        max_retries: максимальное число попыток
        base_delay: начальная задержка между попытками (сек)
        max_delay: максимальная задержка между попытками (сек)

    Returns:
        результат операции

    Raises:
        последнее исключение после исчерпания попыток
    """
    import time

    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                logger.warning(f"Operation failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                logger.error(f"Operation failed after {max_retries} attempts: {e}")
                raise last_exception


# ---------------------------------------------------------------------------
# Создание топиков для истории и состояния
# ---------------------------------------------------------------------------
def ensure_topics(bootstrap_servers: str) -> None:
    """Создаёт необходимые топики если они не существуют (с retry на подключение)."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    def _create_topics():
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            # Топик для истории батчей (обычный, с retention)
            history_topic = NewTopic(
                name=KafkaBatchHistory.HISTORY_TOPIC,
                num_partitions=1,
                replication_factor=1,
            )

            # Топик для состояния (compact - храним только последнее значение)
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

            for topic in [history_topic, state_topic]:
                try:
                    admin_client.create_topics([topic])
                    logger.info(f"Created topic: {topic.name}")
                except TopicAlreadyExistsError:
                    logger.debug(f"Topic already exists: {topic.name}")
        finally:
            admin_client.close()

    _with_retry(_create_topics, max_retries=5, base_delay=1.0)


# ---------------------------------------------------------------------------
# Kafka history - пишет историю в отдельный топик
# ---------------------------------------------------------------------------
class KafkaBatchHistory:
    """Хранение истории batch в Kafka (отдельный топик) с retry."""

    HISTORY_TOPIC = "generator_batch_history"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka с retry."""
        KafkaProducerCls, KafkaErrorCls = _import_kafka()

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

        _with_retry(_do_connect, max_retries=5, base_delay=1.0)

    def add(self, record: BatchRecord):
        """Добавляет запись в историю (топик Kafka) с retry и реконнектом."""
        key = record.batch_id
        value = record.to_dict()

        def _do_send():
            self.producer.send(self.HISTORY_TOPIC, key=key, value=value)

        try:
            _with_retry(_do_send, max_retries=3, base_delay=0.5)
        except Exception as e:
            logger.warning(f"Failed to send history record after retries: {e}, attempting reconnect")
            self._connect()
            # Повторная попытка после реконнекта
            _with_retry(_do_send, max_retries=2, base_delay=0.5)

    def flush(self):
        """Сбрасывает буфер с retry."""
        def _do_flush():
            self.producer.flush()

        _with_retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self):
        """Закрывает соединение."""
        try:
            if self.producer:
                self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing history producer (ignored): {e}")


# ---------------------------------------------------------------------------
# Kafka publisher
# ---------------------------------------------------------------------------
class KafkaPublisher:
    """Публикация событий в Kafka с retry и реконнектом."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka с retry."""
        KafkaProducerCls, KafkaErrorCls = _import_kafka()

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

        _with_retry(_do_connect, max_retries=5, base_delay=1.0)

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

        # Ждём подтверждений
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
        """
        Публикует события в топик с retry и автоматическим реконнектом.

        Returns:
            (sent_count, error_count)
        """
        def _do_publish():
            return self._publish_with_retry(topic, events)

        try:
            return _with_retry(_do_publish, max_retries=3, base_delay=0.5)
        except Exception as e:
            logger.warning(f"Publish failed after retries: {e}, attempting reconnect")
            self._connect()
            # Повторная попытка после реконнекта
            return _with_retry(_do_publish, max_retries=2, base_delay=0.5)

    def flush(self):
        """Сбрасывает буфер с retry."""
        def _do_flush():
            if self.producer:
                self.producer.flush()

        _with_retry(_do_flush, max_retries=3, base_delay=0.5)

    def close(self):
        """Закрывает соединение."""
        try:
            if self.producer:
                self.producer.close()
        except Exception as e:
            logger.debug(f"Error closing producer (ignored): {e}")


# ---------------------------------------------------------------------------
# Основной цикл генератора
# ---------------------------------------------------------------------------
class GeneratorService:
    """Основной сервис генератора."""

    def __init__(self, config: Config):
        self.config = config
        self.dictionary = EventDictionary.load(config.data_dir)
        self.generator = EventGenerator(self.dictionary, config)
        self.publisher: KafkaPublisher | None = None
        self.history: KafkaBatchHistory | None = None
        self.state_manager: KafkaStateManager | None = None
        self._running = False
        self._tick = 0  # Текущий номер тика (восстанавливается из стейта)

    def start(self):
        """Запускает основной цикл."""
        if not self.config.enabled:
            logger.warning("Generator is disabled (GEN_ENABLED=false)")
            return

        # Запускаем HTTP-сервер для Prometheus метрик
        logger.info(f"Starting metrics server on port {self.config.metrics_port}")
        start_http_server(self.config.metrics_port)

        logger.info("Starting generator service...")
        logger.info(f"Configuration: tick={self.config.tick_seconds}s, "
                   f"lambda_base={self.config.lambda_base_per_min}/min, "
                   f"jitter={self.config.jitter_pct}%, "
                   f"state_enabled={self.config.state_enabled}, "
                   f"state_reset={self.config.state_reset}")

        # Подключаемся к Kafka для публикации событий и истории
        # Сначала создаём топики если нужно
        ensure_topics(self.config.kafka_bootstrap_servers)
        self.publisher = KafkaPublisher(self.config.kafka_bootstrap_servers)
        self.history = KafkaBatchHistory(self.config.kafka_bootstrap_servers)

        # Инициализируем state manager если включено
        if self.config.state_enabled:
            self.state_manager = KafkaStateManager(self.config.kafka_bootstrap_servers)

            # Восстанавливаем стейт если не требуется сброс
            if not self.config.state_reset:
                restored_state = self.state_manager.load()
                if restored_state:
                    self._tick = restored_state.tick
                    self.generator.rng.setstate(restored_state.rng_state)
                    logger.info(f"Restored state: continuing from tick {self._tick}, "
                               f"last_batch_id={restored_state.last_batch_id}")
            else:
                logger.info("State reset requested, starting fresh")
        else:
            logger.info("State management disabled")

        self._running = True

        try:
            self._main_loop()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Останавливает сервис."""
        logger.info("Stopping generator service...")
        self._running = False
        if self.publisher:
            self.publisher.close()
        if self.history:
            self.history.close()
        if self.state_manager:
            self.state_manager.close()

    def _save_state(self, batch_id: str) -> None:
        """Сохраняет текущее состояние генератора."""
        if not self.state_manager or not self.config.state_enabled:
            return

        try:
            state = GeneratorState(
                tick=self._tick,
                rng_state=self.generator.rng.getstate(),
                last_batch_id=batch_id,
                last_timestamp=datetime.now(timezone.utc),
            )
            self.state_manager.save(state)
            self.state_manager.flush()
            logger.debug(f"Saved state: tick={self._tick}, batch_id={batch_id}")
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
            METRICS_ERRORS_TOTAL.labels(topic="state").inc()

    def _main_loop(self):
        """Основной цикл тиков."""
        while self._running:
            self._tick += 1
            tick_start = time.time()
            batch_id = str(uuid.uuid4())[:8]

            with METRICS_TICK_DURATION.time():
                logger.info(f"=== Tick {self._tick} (batch_id={batch_id}) ===")

                try:
                    # Вычисляем количество событий
                    events_count = self.generator._calculate_events_count()
                    logger.info(f"Generating ~{events_count} base events")

                    # Генерируем батч
                    gen_start = time.time()
                    batch = self.generator.generate_batch(events_count)
                    gen_duration = time.time() - gen_start

                    # Публикуем в Kafka
                    pub_start = time.time()
                    total_sent = 0
                    total_errors = 0

                    sent_counts = {}
                    for topic, events in batch.items():
                        if events:
                            sent, errors = self.publisher.publish(topic, events)
                            sent_counts[topic] = {"sent": sent, "errors": errors}
                            total_sent += sent
                            total_errors += errors

                    # Определяем статус
                    if total_errors == 0:
                        status = "success"
                    elif total_sent > 0:
                        status = "partial"
                    else:
                        status = "error"

                    # Обновляем метрику последнего успешного тика и сохраняем стейт
                    if status in ("success", "partial"):
                        METRICS_LAST_SUCCESS.set_to_current_time()
                        self._save_state(batch_id)

                    # Флашим публикацию событий
                    self.publisher.flush()
                    pub_duration = time.time() - pub_start

                    # Сохраняем в историю (best-effort: ошибки не валят тик)
                    try:
                        batch_record = BatchRecord(
                            batch_id=batch_id,
                            started_at=datetime.fromtimestamp(tick_start, tz=timezone.utc),
                            finished_at=datetime.now(timezone.utc),
                            sent_total=total_sent,
                            sent_browser=sent_counts.get("browser_events", {}).get("sent", 0),
                            sent_location=sent_counts.get("location_events", {}).get("sent", 0),
                            sent_device=sent_counts.get("device_events", {}).get("sent", 0),
                            sent_geo=sent_counts.get("geo_events", {}).get("sent", 0),
                            status=status,
                            error_message=None if status == "success" else f"Errors: {total_errors}",
                        )
                        self.history.add(batch_record)
                        self.history.flush()
                    except Exception as hist_err:
                        logger.warning(f"Failed to write batch history: {hist_err}")
                        METRICS_ERRORS_TOTAL.labels(topic="history").inc()

                    # Логируем результат
                    tick_duration = time.time() - tick_start
                    logger.info(
                        f"Batch {batch_id} completed: "
                        f"sent={total_sent}, errors={total_errors}, "
                        f"gen_time={gen_duration:.3f}s, pub_time={pub_duration:.3f}s, "
                        f"total_time={tick_duration:.3f}s"
                    )

                    for topic, counts in sent_counts.items():
                        if counts["sent"] > 0:
                            logger.info(f"  {topic}: {counts['sent']} sent")

                except Exception as e:
                    logger.exception(f"Error in tick {self._tick}: {e}")
                    # Пытаемся записать ошибку в историю (best-effort)
                    try:
                        self.history.add(
                            BatchRecord(
                                batch_id=batch_id,
                                started_at=datetime.fromtimestamp(tick_start, tz=timezone.utc),
                                finished_at=datetime.now(timezone.utc),
                                sent_total=0,
                                sent_browser=0,
                                sent_location=0,
                                sent_device=0,
                                sent_geo=0,
                                status="error",
                                error_message=str(e),
                            )
                        )
                        self.history.flush()
                    except Exception as hist_err:
                        logger.warning(f"Failed to write error to history: {hist_err}")

            # Ждём до следующего тика
            elapsed = time.time() - tick_start
            sleep_time = max(0, self.config.tick_seconds - elapsed)
            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.1f}s until next tick")
                time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def main():
    try:
        config = Config()
        service = GeneratorService(config)
        service.start()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
