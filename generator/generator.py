#!/usr/bin/env python3
"""
Автономный генератор событий для Kafka (MVP).

Режим 'steady': каждую минуту публикуем фиксированный объём событий
с небольшой вариативностью (Poisson + jitter).
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

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
        default_factory=lambda: int(os.getenv("GEN_TICK_SECONDS", "60"))
    )
    lambda_base_per_min: int = field(
        default_factory=lambda: int(os.getenv("GEN_LAMBDA_BASE_PER_MIN", "200"))
    )
    jitter_pct: int = field(
        default_factory=lambda: int(os.getenv("GEN_JITTER_PCT", "20"))
    )
    min_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MIN_EVENTS_PER_TICK", "50"))
    )
    max_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MAX_EVENTS_PER_TICK", "500"))
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

    # История batch (ClickHouse)
    clickhouse_host: str = field(
        default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "clickhouse")
    )
    clickhouse_port: int = field(
        default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "9000"))
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
    location_by_event_id: dict[str, dict] = field(default_factory=dict)
    device_by_click_id: dict[str, dict] = field(default_factory=dict)
    geo_by_click_id: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self):
        # Строим индексы для связности
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

        return cls(
            browser_events=load_jsonl("browser_events.jsonl"),
            location_events=load_jsonl("location_events.jsonl"),
            device_events=load_jsonl("device_events.jsonl"),
            geo_events=load_jsonl("geo_events.jsonl"),
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
        # Остальное: 1.0
        if 9 <= hour <= 18:
            return 1.2
        elif 0 <= hour <= 5:
            return 0.7
        return 1.0

    def _calculate_events_count(self) -> int:
        """Вычисляет количество событий для текущего тика (Poisson + ограничения)."""
        import math

        # Базовая интенсивность с учётом часа
        lambda_t = self.config.lambda_base_per_min * self._hour_factor()

        # Масштабируем на длительность тика
        lambda_tick = lambda_t * (self.config.tick_seconds / 60.0)

        # Генерируем Poisson
        # Используем numpy-style подход через exponential
        count = 0
        L = math.exp(-lambda_tick)
        p = 1.0
        while p > L:
            p *= self.rng.random()
            count += 1
        count -= 1

        # Применяем границы
        count = max(self.config.min_events_per_tick, min(count, self.config.max_events_per_tick))

        return count

    def generate_batch(self, batch_size: int) -> dict[str, list[dict]]:
        """
        Генерирует батч событий с сохранением связей.

        Возвращает словарь {topic: [events]}
        """
        batch = {
            "browser_events": [],
            "location_events": [],
            "device_events": [],
            "geo_events": [],
        }

        for _ in range(batch_size):
            # Выбираем случайное браузерное событие как базу
            base_browser = self.rng.choice(self.dictionary.browser_events)
            base_location = self.dictionary.location_by_event_id.get(base_browser["event_id"])
            base_device = self.dictionary.device_by_click_id.get(base_browser["click_id"])
            base_geo = self.dictionary.geo_by_click_id.get(base_browser["click_id"])

            # Генерируем новые ID
            new_event_id = self._new_uuid()
            new_click_id = self._new_uuid()
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
# Batch history (мета-информация)
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


class BatchHistory:
    """Хранение истории batch (пока в памяти, потом в ClickHouse)."""

    def __init__(self):
        self.batches: list[BatchRecord] = []

    def add(self, record: BatchRecord):
        self.batches.append(record)
        # Храним последние 1000 batch
        if len(self.batches) > 1000:
            self.batches = self.batches[-1000:]

    def get_stats(self) -> dict:
        """Возвращает статистику по истории."""
        if not self.batches:
            return {}
        total = len(self.batches)
        success = sum(1 for b in self.batches if b.status == "success")
        return {
            "total_batches": total,
            "success_rate": success / total if total > 0 else 0,
            "last_batch_status": self.batches[-1].status if self.batches else None,
        }


# ---------------------------------------------------------------------------
# Kafka publisher
# ---------------------------------------------------------------------------
class KafkaPublisher:
    """Публикация событий в Kafka."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: KafkaProducer | None = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Небольшая буферизация для производительности
                batch_size=16384,
                linger_ms=100,
                retries=3,
                retry_backoff_ms=1000,
            )
            logger.info("Connected to Kafka successfully")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def publish(self, topic: str, events: list[dict]) -> tuple[int, int]:
        """
        Публикует события в топик.

        Returns:
            (sent_count, error_count)
        """
        if not self.producer:
            raise RuntimeError("Producer not connected")

        sent = 0
        errors = 0
        futures = []

        for event in events:
            # Используем event_id или click_id как ключ для партиционирования
            key = event.get("event_id") or event.get("click_id")
            try:
                future = self.producer.send(topic, key=key, value=event)
                futures.append(future)
            except KafkaError as e:
                logger.error(f"Failed to send message to {topic}: {e}")
                errors += 1

        # Ждём подтверждений
        for future in futures:
            try:
                future.get(timeout=10)
                sent += 1
            except KafkaError as e:
                logger.error(f"Failed to confirm message delivery: {e}")
                errors += 1

        return sent, errors

    def flush(self):
        """Сбрасывает буфер."""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Закрывает соединение."""
        if self.producer:
            self.producer.close()


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
        self.history = BatchHistory()
        self._running = False

    def start(self):
        """Запускает основной цикл."""
        if not self.config.enabled:
            logger.warning("Generator is disabled (GEN_ENABLED=false)")
            return

        logger.info("Starting generator service...")
        logger.info(f"Configuration: tick={self.config.tick_seconds}s, "
                   f"lambda_base={self.config.lambda_base_per_min}, "
                   f"jitter={self.config.jitter_pct}%")

        self.publisher = KafkaPublisher(self.config.kafka_bootstrap_servers)
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

    def _main_loop(self):
        """Основной цикл тиков."""
        tick = 0

        while self._running:
            tick += 1
            tick_start = time.time()
            batch_id = str(uuid.uuid4())[:8]

            logger.info(f"=== Tick {tick} (batch_id={batch_id}) ===")

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

                self.publisher.flush()
                pub_duration = time.time() - pub_start

                # Определяем статус
                if total_errors == 0:
                    status = "success"
                elif total_sent > 0:
                    status = "partial"
                else:
                    status = "error"

                # Сохраняем в историю
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

                # Логируем результат
                tick_duration = time.time() - tick_start
                logger.info(
                    f"Batch {batch_id} completed: "
                    f"sent={total_sent}, errors={total_errors}, "
                    f"gen_time={gen_duration:.3f}s, pub_time={pub_duration:.3f}s, "
                    f"total_time={tick_duration:.3f}s"
                )

                # Выводим детализацию по топикам
                for topic, counts in sent_counts.items():
                    if counts["sent"] > 0:
                        logger.info(f"  {topic}: {counts['sent']} sent")

            except Exception as e:
                logger.exception(f"Error in tick {tick}: {e}")
                # Сохраняем ошибку в историю
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

            # Ждём до следующего тика
            elapsed = time.time() - tick_start
            sleep_time = max(0, self.config.tick_seconds - elapsed)
            if sleep_time > 0:
                logger.info(f"Sleeping for {sleep_time:.1f}s until next tick")
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
