"""Основной сервисный цикл генератора."""

import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from prometheus_client import start_http_server

from clickstream_generator.config import Config
from clickstream_generator.dictionary import EventDictionary
from clickstream_generator.generation import EventGenerator
from clickstream_generator.kafka_io import (
    BatchRecord,
    KafkaBatchHistory,
    KafkaPublisher,
    KafkaStateManager,
    ensure_topics,
)
from clickstream_generator.metrics import (
    METRICS_ERRORS_TOTAL,
    METRICS_LAST_SUCCESS,
    METRICS_TICK_DURATION,
)
from clickstream_generator.runtime import TickStreamGenerator


logger = logging.getLogger("generator")


class GeneratorService:
    """Основной сервис генератора."""

    def __init__(self, config: Config):
        self.config = config
        self.dictionary = EventDictionary.load(config.data_dir)
        self.generator = EventGenerator(self.dictionary, config)
        self.stream = TickStreamGenerator(self.generator)
        self.publisher: KafkaPublisher | None = None
        self.history: KafkaBatchHistory | None = None
        self.state_manager: KafkaStateManager | None = None
        self._running = False
        self._tick = 0
        self._model_time = config.model_t0

    def start(self):
        """Запускает основной цикл."""
        if not self.config.enabled:
            logger.warning("Generator is disabled (GEN_ENABLED=false)")
            return

        logger.info(f"Starting metrics server on port {self.config.metrics_port}")
        start_http_server(self.config.metrics_port)

        logger.info("Starting generator service...")
        logger.info(
            f"Configuration: tick={self.config.tick_seconds}s, "
            f"lambda_base={self.config.lambda_base_per_min}/min, "
            f"jitter={self.config.jitter_pct}%, "
            f"state_enabled={self.config.state_enabled}, "
            f"state_reset={self.config.state_reset}"
        )

        ensure_topics(self.config.kafka_bootstrap_servers)
        self.publisher = KafkaPublisher(self.config.kafka_bootstrap_servers)
        self.history = KafkaBatchHistory(self.config.kafka_bootstrap_servers)

        if self.config.state_enabled:
            self.state_manager = KafkaStateManager(self.config.kafka_bootstrap_servers)

            if not self.config.state_reset:
                restored_state = self.state_manager.load()
                if restored_state:
                    try:
                        self._restore_live_state(
                            restored_state,
                            wall_now_utc=datetime.now(timezone.utc),
                        )
                        logger.info(
                            f"Restored state: continuing from tick {self._tick}, "
                            f"model_time={self._model_time.isoformat()}, "
                            f"last_batch_id={restored_state.last_batch_id}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"State data was invalid, starting fresh: {e}"
                        )
                        self.generator = EventGenerator(self.dictionary, self.config)
                        self.stream = TickStreamGenerator(self.generator)
                        self._tick = 0
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

    def restore_from_startup_history(
        self,
        state,
        model_t_end: datetime,
    ) -> None:
        """Восстанавливает слепок стартовой истории ровно от T_end."""
        self._restore_state_snapshot(state, resume_model_at=model_t_end)

    def _restore_live_state(self, state, wall_now_utc: datetime) -> None:
        """Восстанавливает live-state с учётом прошедшего настенного времени."""
        self._validate_live_state_config(state)
        resume_model_at = self._calculate_live_resume_model_at(
            state,
            wall_now_utc=wall_now_utc,
        )
        self._restore_state_snapshot(state, resume_model_at=resume_model_at)

    def _restore_state_snapshot(self, state, resume_model_at: datetime) -> None:
        """Применяет state к генератору и тиковому слою."""
        resume_model_at = self._as_aware_utc(resume_model_at)
        self.generator.rng.setstate(state.rng_state)
        self.stream.restore_state(state, resume_model_at=resume_model_at)
        self._tick = state.tick
        self._model_time = resume_model_at

    def _calculate_live_resume_model_at(
        self,
        state,
        wall_now_utc: datetime,
    ) -> datetime:
        """Считает модельную точку live-восстановления по state v2."""
        wall_now_utc = self._as_aware_utc(wall_now_utc)
        wall_saved_at = self._as_aware_utc(state.wall_timestamp)
        idle_seconds = max(0.0, (wall_now_utc - wall_saved_at).total_seconds())
        return self._as_aware_utc(state.model_timestamp) + timedelta(
            seconds=idle_seconds * state.model_time_speed,
        )

    def _validate_live_state_config(self, state) -> None:
        """Проверяет, что state относится к текущей конфигурации live-запуска."""
        mismatches = []
        if state.gen_seed != self.config.seed:
            mismatches.append("gen_seed")
        if self._as_aware_utc(state.model_t0) != self.config.model_t0:
            mismatches.append("model_t0")
        if state.model_timezone != self.config.model_timezone:
            mismatches.append("model_timezone")
        if abs(state.model_time_speed - self.config.model_time_speed) > 1e-9:
            mismatches.append("model_time_speed")

        if mismatches:
            raise ValueError(
                "state config mismatch: " + ", ".join(mismatches)
            )

    @staticmethod
    def _as_aware_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _save_state(self, batch_id: str) -> None:
        """Сохраняет текущее состояние генератора."""
        if not self.state_manager or not self.config.state_enabled:
            return

        try:
            wall_now = datetime.now(timezone.utc)
            state = self.stream.to_state(
                tick=self._tick,
                rng_state=self.generator.rng.getstate(),
                last_batch_id=batch_id,
                last_timestamp=self._model_time,
                model_timestamp=self._model_time,
                wall_timestamp=wall_now,
                model_time_speed=self.config.model_time_speed,
                model_timezone=self.config.model_timezone,
                model_t0=self.config.model_t0,
                gen_seed=self.config.seed,
            )
            self.state_manager.save(state)
            self.state_manager.flush()
            logger.debug(
                f"Saved state: tick={self._tick}, "
                f"model_time={self._model_time.isoformat()}, batch_id={batch_id}"
            )
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
            METRICS_ERRORS_TOTAL.labels(topic="state").inc()

    def _main_loop(self):
        """Основной цикл тиков."""
        while self._running:
            self._tick += 1
            tick_start = time.time()
            batch_id = str(uuid.uuid4())[:8]
            model_time = self._model_time

            with METRICS_TICK_DURATION.time():
                logger.info(f"=== Tick {self._tick} (batch_id={batch_id}) ===")

                try:
                    events_count = self.generator._calculate_events_count(
                        now=model_time,
                    )
                    logger.info(f"Generating with event budget ~{events_count}")

                    gen_start = time.time()
                    batch = self.stream.generate_tick(
                        events_count,
                        tick_started_at=model_time,
                    )
                    gen_duration = time.time() - gen_start

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

                    if total_errors == 0:
                        status = "success"
                    elif total_sent > 0:
                        status = "partial"
                    else:
                        status = "error"

                    if status in ("success", "partial"):
                        METRICS_LAST_SUCCESS.set_to_current_time()
                        self._advance_model_time()
                        self._save_state(batch_id)

                    self.publisher.flush()
                    pub_duration = time.time() - pub_start

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

            elapsed = time.time() - tick_start
            sleep_time = max(0, self.config.tick_seconds - elapsed)
            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.1f}s until next tick")
                time.sleep(sleep_time)

    def _advance_model_time(self) -> None:
        """Сдвигает модельное время после успешного live-тика."""
        self._model_time = self._model_time + timedelta(
            seconds=self.config.tick_seconds * self.config.model_time_speed,
        )


def main():
    """Точка входа сервиса."""
    try:
        config = Config()
        service = GeneratorService(config)
        service.start()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
