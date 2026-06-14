"""Основной сервисный цикл генератора."""

import hashlib
import json
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
    KafkaStartupHistoryManifest,
    ensure_topics,
)
from clickstream_generator.metrics import (
    METRICS_ERRORS_TOTAL,
    METRICS_LAST_SUCCESS,
    METRICS_TICK_DURATION,
)
from clickstream_generator.runtime import TickStreamGenerator


logger = logging.getLogger("generator")


class _TopicManifestStats:
    """Накопительные счётчики одного Kafka-топика для manifest."""

    def __init__(self):
        self.rows = 0
        self.min_event_timestamp: str | None = None
        self.max_event_timestamp: str | None = None
        self._checksum = hashlib.sha256()

    @property
    def checksum(self) -> str:
        return self._checksum.hexdigest()

    def add(self, event: dict, event_timestamp: str | None = None) -> None:
        self.rows += 1
        self._checksum.update(
            json.dumps(event, sort_keys=True, ensure_ascii=True).encode("utf-8")
        )
        timestamp = event_timestamp if event_timestamp is not None else event.get("event_timestamp")
        self.add_timestamp(timestamp)

    def add_timestamp(self, timestamp: str | None) -> None:
        if timestamp is None:
            return
        if self.min_event_timestamp is None or timestamp < self.min_event_timestamp:
            self.min_event_timestamp = timestamp
        if self.max_event_timestamp is None or timestamp > self.max_event_timestamp:
            self.max_event_timestamp = timestamp

    def to_dict(self) -> dict:
        return {
            "rows": self.rows,
            "min_event_timestamp": self.min_event_timestamp,
            "max_event_timestamp": self.max_event_timestamp,
            "checksum_sha256": self.checksum,
        }


class _ManifestCounters:
    """Счётчики стартовой истории для manifest."""

    def __init__(self):
        self.topic_stats = {
            topic: _TopicManifestStats()
            for topic in ("browser_events", "location_events", "device_events", "geo_events")
        }
        self.click_ids: set[str] = set()
        self.user_ids: set[str] = set()

    def add_batch(self, batch: dict[str, list[dict]]) -> None:
        browser_events = batch.get("browser_events", [])
        event_timestamps = {
            event["event_id"]: event.get("event_timestamp")
            for event in browser_events
            if event.get("event_id") and event.get("event_timestamp")
        }
        click_timestamps: dict[str, list[str]] = {}
        for event in browser_events:
            click_id = event.get("click_id")
            timestamp = event.get("event_timestamp")
            if click_id and timestamp:
                click_timestamps.setdefault(click_id, []).append(timestamp)

        for topic, events in batch.items():
            stats = self.topic_stats[topic]
            for event in events:
                event_timestamp = event.get("event_timestamp")
                if event_timestamp is None and topic == "location_events":
                    event_timestamp = event_timestamps.get(event.get("event_id"))
                if event_timestamp is None and topic in ("device_events", "geo_events"):
                    timestamps = click_timestamps.get(event.get("click_id"))
                    if timestamps:
                        event_timestamp = min(timestamps)
                        stats.add_timestamp(max(timestamps))
                stats.add(event, event_timestamp=event_timestamp)
                click_id = event.get("click_id")
                if topic == "browser_events" and click_id:
                    self.click_ids.add(click_id)
                user_id = event.get("user_domain_id")
                if topic == "device_events" and user_id:
                    self.user_ids.add(user_id)

    def to_manifest_topics(self) -> dict:
        return {
            topic: stats.to_dict()
            for topic, stats in self.topic_stats.items()
        }

    def to_manifest_totals(self) -> dict:
        browser_stats = self.topic_stats["browser_events"]
        return {
            "events": browser_stats.rows,
            "visits": len(self.click_ids),
            "users": len(self.user_ids),
            "min_event_timestamp": browser_stats.min_event_timestamp,
            "max_event_timestamp": browser_stats.max_event_timestamp,
        }


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
        self.manifest_manager: KafkaStartupHistoryManifest | None = None
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

        if self.config.run_mode == "backfill" and not self.config.state_enabled:
            raise ValueError("GEN_STATE_ENABLED must be true for backfill")

        if self.config.state_enabled:
            self.state_manager = KafkaStateManager(self.config.kafka_bootstrap_servers)

            if self.config.run_mode == "backfill":
                self.manifest_manager = KafkaStartupHistoryManifest(
                    self.config.kafka_bootstrap_servers
                )
                logger.info("Backfill mode starts from a fresh generator state")
                self._run_backfill()
                self.stop()
                return

            if not self.config.state_reset:
                restored_state = self.state_manager.load()
                if restored_state:
                    try:
                        self.manifest_manager = KafkaStartupHistoryManifest(
                            self.config.kafka_bootstrap_servers
                        )
                        manifest = self.manifest_manager.load()
                        if self._is_startup_history_state(restored_state, manifest):
                            model_t_end = self._as_aware_utc(
                                datetime.fromisoformat(manifest["model_t_end"])
                            )
                            self.restore_from_startup_history(
                                restored_state,
                                model_t_end=model_t_end,
                            )
                        elif self._is_startup_history_marker(restored_state):
                            raise ValueError(
                                "startup-history state without matching manifest"
                            )
                        else:
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
        if self.manifest_manager:
            self.manifest_manager.close()

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

    def _is_startup_history_state(self, state, manifest: dict | None) -> bool:
        """Проверяет, что state совпадает со слепком стартовой истории."""
        if not manifest or manifest.get("run_mode") != "backfill":
            return False

        expected_state = manifest.get("state") or {}
        if expected_state.get("last_batch_id") != state.last_batch_id:
            return False

        try:
            model_t_end = self._as_aware_utc(
                datetime.fromisoformat(manifest["model_t_end"])
            )
            model_t0 = self._as_aware_utc(
                datetime.fromisoformat(manifest["model_t0"])
            )
        except (KeyError, TypeError, ValueError):
            return False

        if self._as_aware_utc(state.model_timestamp) != model_t_end:
            return False
        if self._as_aware_utc(state.model_t0) != model_t0:
            return False
        if state.gen_seed != manifest.get("gen_seed"):
            return False
        if state.model_timezone != manifest.get("model_timezone"):
            return False
        if (
            self.config.model_t_end is not None
            and self.config.model_t_end != model_t_end
        ):
            return False
        if model_t0 != self.config.model_t0:
            return False
        if manifest.get("gen_seed") != self.config.seed:
            return False
        if manifest.get("model_timezone") != self.config.model_timezone:
            return False

        settings = manifest.get("generation_settings") or {}
        if abs(state.model_time_speed - settings.get("model_time_speed", -1)) > 1e-9:
            return False
        return self._generation_settings() == settings

    def _is_startup_history_marker(self, state) -> bool:
        """Отличает state стартовой истории от обычного live-state."""
        return str(state.last_batch_id).startswith("startup-history-")

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

    def _run_backfill(self) -> None:
        """Проматывает стартовую историю без сна до GEN_MODEL_T_END."""
        if self.config.model_t_end is None:
            raise ValueError("GEN_MODEL_T_END is required for backfill")
        if not self.publisher:
            raise RuntimeError("publisher is not initialized")
        if not self.history:
            raise RuntimeError("history is not initialized")

        logger.info(
            "Running backfill from %s to %s",
            self.config.model_t0.isoformat(),
            self.config.model_t_end.isoformat(),
        )
        counters = _ManifestCounters()

        while self._model_time < self.config.model_t_end:
            self._tick += 1
            batch_id = f"backfill-{self._tick:08d}"
            model_time = self._model_time
            started_at = datetime.now(timezone.utc)

            events_count = self.generator._calculate_events_count(now=model_time)
            batch = self.stream.generate_tick(
                events_count,
                tick_started_at=model_time,
            )
            total_sent, sent_counts, status = self._publish_batch(batch)
            self._raise_on_backfill_publish_error(batch_id, status, sent_counts)
            counters.add_batch(batch)
            self._write_batch_history(
                batch_id=batch_id,
                started_at=started_at,
                sent_counts=sent_counts,
                total_sent=total_sent,
                status=status,
                error_message=None if status == "success" else "Backfill publish error",
            )
            self._advance_model_time()

        final_batch = self.stream.drain_until(
            self.config.model_t_end,
            include_boundary=False,
        )
        final_sent = 0
        final_status = "success"
        if any(final_batch.values()):
            batch_id = f"backfill-{self._tick + 1:08d}-final"
            started_at = datetime.now(timezone.utc)
            final_sent, sent_counts, final_status = self._publish_batch(final_batch)
            self._raise_on_backfill_publish_error(
                batch_id,
                final_status,
                sent_counts,
            )
            counters.add_batch(final_batch)
            self._write_batch_history(
                batch_id=batch_id,
                started_at=started_at,
                sent_counts=sent_counts,
                total_sent=final_sent,
                status=final_status,
                error_message=None if final_status == "success" else "Backfill publish error",
            )

        if self.publisher:
            self.publisher.flush()

        self._model_time = self.config.model_t_end
        state_batch_id = self._startup_state_batch_id(counters)
        state = self.stream.to_state(
            tick=self._tick,
            rng_state=self.generator.rng.getstate(),
            last_batch_id=state_batch_id,
            last_timestamp=self.config.model_t_end,
            model_timestamp=self.config.model_t_end,
            wall_timestamp=datetime.now(timezone.utc),
            model_time_speed=self.config.model_time_speed,
            model_timezone=self.config.model_timezone,
            model_t0=self.config.model_t0,
            gen_seed=self.config.seed,
        )

        manifest = self._build_startup_history_manifest(
            counters=counters,
            state=state,
        )
        if self.manifest_manager:
            self.manifest_manager.save(manifest)
            self.manifest_manager.flush()

        if self.state_manager and self.config.state_enabled:
            self.state_manager.save(state)
            self.state_manager.flush()

        logger.info(
            "Backfill completed: events=%s, visits=%s, users=%s, final_sent=%s",
            manifest["totals"]["events"],
            manifest["totals"]["visits"],
            manifest["totals"]["users"],
            final_sent,
        )

    def _raise_on_backfill_publish_error(
        self,
        batch_id: str,
        status: str,
        sent_counts: dict[str, dict[str, int]],
    ) -> None:
        """Останавливает backfill до записи state/manifest при ошибке Kafka."""
        if status == "success":
            return

        raise RuntimeError(
            f"Backfill publish failed for batch {batch_id}: "
            f"status={status}, sent_counts={sent_counts}"
        )

    def _publish_batch(
        self,
        batch: dict[str, list[dict]],
    ) -> tuple[int, dict[str, dict[str, int]], str]:
        """Публикует batch и возвращает счётчики отправки."""
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

        return total_sent, sent_counts, status

    def _write_batch_history(
        self,
        batch_id: str,
        started_at: datetime,
        sent_counts: dict[str, dict[str, int]],
        total_sent: int,
        status: str,
        error_message: str | None,
    ) -> None:
        """Пишет служебную историю batch."""
        if not self.history:
            return
        try:
            record = BatchRecord(
                batch_id=batch_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                sent_total=total_sent,
                sent_browser=sent_counts.get("browser_events", {}).get("sent", 0),
                sent_location=sent_counts.get("location_events", {}).get("sent", 0),
                sent_device=sent_counts.get("device_events", {}).get("sent", 0),
                sent_geo=sent_counts.get("geo_events", {}).get("sent", 0),
                status=status,
                error_message=error_message,
            )
            self.history.add(record)
            self.history.flush()
        except Exception as hist_err:
            logger.warning(f"Failed to write batch history: {hist_err}")
            METRICS_ERRORS_TOTAL.labels(topic="history").inc()

    def _startup_state_batch_id(self, counters) -> str:
        digest = counters.topic_stats["browser_events"].checksum[:12]
        return f"startup-history-{digest}"

    def _build_startup_history_manifest(self, counters, state) -> dict:
        return {
            "manifest_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "gen_seed": self.config.seed,
            "model_t0": self.config.model_t0.isoformat(),
            "model_t_end": self.config.model_t_end.isoformat(),
            "model_timezone": self.config.model_timezone,
            "run_mode": "backfill",
            "generation_settings": self._generation_settings(),
            "state_version": state.version,
            "state": {
                "topic": KafkaStateManager.STATE_TOPIC,
                "key": KafkaStateManager.STATE_KEY,
                "last_batch_id": state.last_batch_id,
                "model_timestamp": state.model_timestamp.isoformat(),
            },
            "topics": counters.to_manifest_topics(),
            "totals": counters.to_manifest_totals(),
        }

    def _generation_settings(self) -> dict:
        return {
            "tick_seconds": self.config.tick_seconds,
            "lambda_base_per_min": self.config.lambda_base_per_min,
            "jitter_pct": self.config.jitter_pct,
            "min_events_per_tick": self.config.min_events_per_tick,
            "max_events_per_tick": self.config.max_events_per_tick,
            "max_session_events": self.config.max_session_events,
            "max_active_sessions": self.config.max_active_sessions,
            "population_max": self.config.population_max,
            "p_new_user": self.config.p_new_user,
            "min_return_minutes": self.config.min_return_minutes,
            "model_time_speed": self.config.model_time_speed,
        }

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
