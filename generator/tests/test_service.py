"""
Тесты для GeneratorService и интеграционных сценариев.
"""

import logging
import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import sleep as real_sleep
from unittest.mock import MagicMock, patch

import pytest
from generator import (
    Config,
    EventDictionary,
    EventGenerator,
    EXPECTED_VISIT_EVENTS,
    GeneratorService,
    GeneratorState,
    KafkaBatchHistory,
    TickStreamGenerator,
)


class TestBatchRecordWithDictConversion:
    """Тесты конвертации BatchRecord в dict."""

    def test_dict_contains_all_batch_info(self):
        """Словарь содержит всю информацию о батче."""
        from datetime import datetime, timezone
        from generator import BatchRecord

        started = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2024, 6, 15, 12, 0, 5, tzinfo=timezone.utc)

        record = BatchRecord(
            batch_id="batch_001",
            started_at=started,
            finished_at=finished,
            sent_total=400,
            sent_browser=100,
            sent_location=100,
            sent_device=100,
            sent_geo=100,
            status="success",
            error_message=None,
        )

        data = record.to_dict()

        # Проверяем структуру JSON
        assert isinstance(data, dict)
        assert data["batch_id"] == "batch_001"
        assert data["sent_total"] == 400
        assert data["status"] == "success"
        assert "started_at" in data
        assert "finished_at" in data

        # Проверяем что можно сериализовать в JSON
        import json
        json_str = json.dumps(data)
        assert isinstance(json_str, str)

        # Проверяем что можно десериализовать
        restored = json.loads(json_str)
        assert restored["batch_id"] == "batch_001"


class TestGeneratorServiceInit:
    """Тесты инициализации GeneratorService."""

    def test_service_initializes_dictionary(self, base_config):
        """Service загружает словарь при инициализации."""
        service = GeneratorService(base_config)
        
        assert service.dictionary is not None
        assert len(service.dictionary.browser_events) == 1000
        assert service.generator is not None
        assert service.config == base_config

    def test_service_history_is_none_before_start(self, base_config):
        """История None до вызова start."""
        service = GeneratorService(base_config)
        
        # История и publisher инициализируются в start()
        assert service.history is None
        assert service.publisher is None


class TestGeneratorServiceDisabled:
    """Тесты отключенного генератора."""

    def test_disabled_generator_logs_warning(self, base_config, caplog):
        """Отключенный генератор логирует warning."""
        from dataclasses import replace
        import logging
        
        disabled_config = replace(base_config, enabled=False)
        service = GeneratorService(disabled_config)
        
        with caplog.at_level(logging.WARNING):
            service.start()
        
        assert "disabled" in caplog.text.lower() or "GEN_ENABLED" in caplog.text


class TestGeneratorServiceSteadyStream:
    """Проверки сервисного тика без настоящей Kafka."""

    def test_service_live_tick_uses_model_time_for_events_and_day_factor(self, base_config):
        """Живой тик пишет события от T0 и не зависит от реального часа запуска."""
        model_t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        config = replace(
            base_config,
            tick_seconds=60,
            lambda_base_per_min=600,
            jitter_pct=0,
            min_events_per_tick=1,
            max_events_per_tick=1000,
            max_session_events=1,
            max_active_sessions=250,
            population_max=251,
            model_t0=model_t0,
        )

        night_wall_run = self._run_service_ticks(
            config,
            wall_now=datetime(2026, 6, 14, 3, 0, tzinfo=timezone.utc),
            ticks_count=2,
        )
        day_wall_run = self._run_service_ticks(
            config,
            wall_now=datetime(2026, 6, 14, 11, 0, tzinfo=timezone.utc),
            ticks_count=2,
        )

        assert len(night_wall_run["browser_events"]) == len(day_wall_run["browser_events"])
        assert night_wall_run["budget_model_times"] == [
            datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc),
        ]
        assert day_wall_run["budget_model_times"] == night_wall_run["budget_model_times"]
        assert night_wall_run["browser_events"]
        timestamps = {
            event["event_timestamp"]
            for event in night_wall_run["browser_events"]
        }
        assert timestamps == {
            "2026-01-01 10:00:00.000000",
            "2026-01-01 10:01:00.000000",
        }

    def test_service_live_tick_advances_event_timestamps_by_model_speed(self, base_config):
        """При ×K сервис сдвигает события на ускоренный модельный шаг."""
        model_t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        config = replace(
            base_config,
            tick_seconds=60,
            lambda_base_per_min=600,
            jitter_pct=0,
            min_events_per_tick=1,
            max_events_per_tick=1000,
            max_session_events=1,
            max_active_sessions=250,
            population_max=251,
            model_t0=model_t0,
            model_time_speed=10,
        )

        published = self._run_service_ticks(
            config,
            wall_now=datetime(2026, 6, 14, 11, 0, tzinfo=timezone.utc),
            ticks_count=2,
        )

        timestamps = {
            event["event_timestamp"]
            for event in published["browser_events"]
        }
        assert timestamps == {
            "2026-01-01 10:00:00.000000",
            "2026-01-01 10:10:00.000000",
        }

    def _run_service_ticks(self, config, wall_now: datetime, ticks_count: int):
        service = GeneratorService(config)
        service.publisher = MagicMock()
        service.publisher.publish.side_effect = (
            lambda topic, events: (len(events), 0)
        )
        service.history = MagicMock()
        service._running = True
        sleep_calls = 0
        budget_model_times = []
        original_calculate_events_count = service.generator._calculate_events_count

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return wall_now.replace(tzinfo=None)
                return wall_now.astimezone(tz)

        def stop_after_tick(_sleep_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= ticks_count:
                service._running = False

        def calculate_events_count(now=None):
            budget_model_times.append(now)
            return original_calculate_events_count(now=now)

        with patch("clickstream_generator.service.datetime", FrozenDateTime), \
             patch.object(
                 service.generator,
                 "_calculate_events_count",
                 side_effect=calculate_events_count,
             ), \
             patch("clickstream_generator.service.time.sleep", side_effect=stop_after_tick):
            service._main_loop()

        published = {}
        for call in service.publisher.publish.call_args_list:
            topic, events = call.args
            published.setdefault(topic, []).extend(events)
        published["budget_model_times"] = budget_model_times
        return published

    def test_service_ticks_publish_connected_multi_event_visit(self, base_config):
        """Сервисные тики публикуют несколько связанных событий одного визита."""
        config = replace(base_config, tick_seconds=1, max_session_events=3)
        service = GeneratorService(config)
        service.publisher = MagicMock()
        service.publisher.publish.side_effect = (
            lambda topic, events: (len(events), 0)
        )
        service.history = MagicMock()
        service._running = True
        sleep_calls = 0

        def stop_after_second_tick(sleep_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls == 1:
                real_sleep(sleep_seconds)
            else:
                service._running = False

        with patch.object(
            service.generator,
            "_calculate_events_count",
            side_effect=[int(EXPECTED_VISIT_EVENTS), 0],
        ), patch.object(
            service.generator,
            "_visit_pause_seconds",
            return_value=0.05,
        ), patch("clickstream_generator.service.time.sleep") as sleep_mock:
            sleep_mock.side_effect = stop_after_second_tick

            service._main_loop()

        published = {}
        for call in service.publisher.publish.call_args_list:
            topic, events = call.args
            published.setdefault(topic, []).extend(events)

        browser_events = published["browser_events"]
        location_events = published["location_events"]
        device_events = published["device_events"]
        geo_events = published["geo_events"]

        assert set(published) == {
            "browser_events",
            "location_events",
            "device_events",
            "geo_events",
        }
        assert len(browser_events) >= 2
        assert len({event["click_id"] for event in browser_events}) == 1
        assert len({event["event_id"] for event in browser_events}) == len(browser_events)
        assert {event["event_id"] for event in location_events} == {
            event["event_id"]
            for event in browser_events
        }
        assert {event["click_id"] for event in device_events} == {
            browser_events[0]["click_id"]
        }
        assert {event["click_id"] for event in geo_events} == {
            browser_events[0]["click_id"]
        }

        history_records = [
            call.args[0]
            for call in service.history.add.call_args_list
        ]
        assert [record.status for record in history_records] == ["success", "success"]
        assert sum(record.sent_browser for record in history_records) == len(browser_events)
        assert sum(record.sent_location for record in history_records) == len(location_events)
        assert sum(record.sent_device for record in history_records) == len(device_events)
        assert sum(record.sent_geo for record in history_records) == len(geo_events)


class TestGeneratorServiceStateV2:
    """Тесты подключения state v2 к сервисному запуску."""

    def test_start_restores_tick_stream_state_v2(self, base_config, event_dictionary):
        """Сервис восстанавливает популяцию и активные визиты из state v2."""
        source_generator = EventGenerator(event_dictionary, base_config)
        source_stream = TickStreamGenerator(source_generator)
        tick_at = datetime.now(timezone.utc).replace(tzinfo=None)
        source_stream.generate_tick(event_budget=10, tick_started_at=tick_at)
        state = source_stream.to_state(
            tick=3,
            rng_state=source_generator.rng.getstate(),
            last_batch_id="batch-3",
            last_timestamp=tick_at,
            model_timestamp=tick_at.replace(tzinfo=timezone.utc),
            wall_timestamp=tick_at.replace(tzinfo=timezone.utc),
            model_time_speed=base_config.model_time_speed,
            model_timezone=base_config.model_timezone,
            model_t0=base_config.model_t0,
            gen_seed=base_config.seed,
        )

        state_manager = MagicMock()
        state_manager.load.return_value = state

        with patch("clickstream_generator.service.start_http_server"), \
             patch("clickstream_generator.service.ensure_topics"), \
             patch("clickstream_generator.service.KafkaPublisher"), \
             patch("clickstream_generator.service.KafkaBatchHistory"), \
             patch(
                 "clickstream_generator.service.KafkaStateManager",
                 return_value=state_manager,
             ), \
             patch.object(GeneratorService, "_main_loop", return_value=None):

            service = GeneratorService(base_config)
            service.start()

        assert service._tick == 3
        assert service.stream.population_user_ids == source_stream.population_user_ids
        assert service.stream.active_visit_count == source_stream.active_visit_count

    def test_start_restores_model_time_from_state_wall_delta(
        self, base_config, event_dictionary
    ):
        """Live-восстановление считает точку модели из сохранённой wall-метки."""
        model_t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        wall_saved_at = datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc)
        wall_restarted_at = wall_saved_at + timedelta(seconds=30)
        config = replace(
            base_config,
            model_t0=model_t0,
            model_time_speed=10,
            tick_seconds=60,
        )
        source_generator = EventGenerator(event_dictionary, config)
        source_stream = TickStreamGenerator(source_generator)
        source_stream.generate_tick(event_budget=10, tick_started_at=model_t0)
        state = source_stream.to_state(
            tick=3,
            rng_state=source_generator.rng.getstate(),
            last_batch_id="batch-3",
            last_timestamp=model_t0,
            model_timestamp=model_t0,
            wall_timestamp=wall_saved_at,
            model_time_speed=config.model_time_speed,
            model_timezone=config.model_timezone,
            model_t0=config.model_t0,
            gen_seed=config.seed,
        )
        state_manager = MagicMock()
        state_manager.load.return_value = state

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return wall_restarted_at.replace(tzinfo=None)
                return wall_restarted_at.astimezone(tz)

        with patch("clickstream_generator.service.start_http_server"), \
             patch("clickstream_generator.service.ensure_topics"), \
             patch("clickstream_generator.service.KafkaPublisher"), \
             patch("clickstream_generator.service.KafkaBatchHistory"), \
             patch(
                 "clickstream_generator.service.KafkaStateManager",
                 return_value=state_manager,
             ), \
             patch("clickstream_generator.service.datetime", FrozenDateTime), \
             patch.object(GeneratorService, "_main_loop", return_value=None):

            service = GeneratorService(config)
            service.start()

        assert service._tick == 3
        assert service._model_time == model_t0 + timedelta(seconds=300)
        assert service.stream.active_visit_count == source_stream.active_visit_count

    def test_restore_from_startup_history_uses_passed_model_point_without_wall_delta(
        self, base_config, event_dictionary
    ):
        """Стартовая история продолжает с T_end, а не с wall-простоя."""
        model_t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        model_t_end = model_t0 + timedelta(minutes=5)
        old_wall_saved_at = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        config = replace(
            base_config,
            model_t0=model_t0,
            model_time_speed=3600,
            tick_seconds=60,
            max_session_events=5,
        )
        source_generator = EventGenerator(event_dictionary, config)
        source_stream = TickStreamGenerator(source_generator)
        source_stream.generate_tick(event_budget=10, tick_started_at=model_t_end)
        state = source_stream.to_state(
            tick=99,
            rng_state=source_generator.rng.getstate(),
            last_batch_id="history-end",
            last_timestamp=model_t_end,
            model_timestamp=model_t_end,
            wall_timestamp=old_wall_saved_at,
            model_time_speed=config.model_time_speed,
            model_timezone=config.model_timezone,
            model_t0=config.model_t0,
            gen_seed=config.seed,
        )

        service = GeneratorService(config)
        service.restore_from_startup_history(state, model_t_end=model_t_end)

        assert service._tick == 99
        assert service._model_time == model_t_end
        assert service.stream.active_visit_count == source_stream.active_visit_count

    def test_save_state_writes_tick_stream_state_v2(self, base_config):
        """Сервис сохраняет v2-снимок тикового слоя."""
        service = GeneratorService(base_config)
        service.state_manager = MagicMock()
        tick_at = datetime.now(timezone.utc).replace(tzinfo=None)
        service.stream.generate_tick(event_budget=10, tick_started_at=tick_at)
        service._tick = 1
        service._model_time = tick_at.replace(tzinfo=timezone.utc)

        service._save_state("batch-1")

        saved_state = service.state_manager.save.call_args.args[0]
        assert saved_state.version == "2.0"
        assert saved_state.model_timestamp == tick_at.replace(tzinfo=timezone.utc)
        assert saved_state.wall_timestamp.tzinfo is not None
        assert saved_state.model_time_speed == base_config.model_time_speed
        assert saved_state.model_timezone == base_config.model_timezone
        assert saved_state.model_t0 == base_config.model_t0
        assert saved_state.gen_seed == base_config.seed
        assert saved_state.population
        assert saved_state.active_visits
        service.state_manager.flush.assert_called_once()

    def test_incompatible_seed_state_starts_fresh(self, base_config, event_dictionary, caplog):
        """State от другого GEN_SEED не смешивается с текущим запуском."""
        source_config = replace(base_config, seed=7)
        source_generator = EventGenerator(event_dictionary, source_config)
        source_stream = TickStreamGenerator(source_generator)
        tick_at = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        source_stream.generate_tick(event_budget=10, tick_started_at=tick_at)
        state = source_stream.to_state(
            tick=5,
            rng_state=source_generator.rng.getstate(),
            last_batch_id="other-seed",
            last_timestamp=tick_at,
            model_timestamp=tick_at,
            wall_timestamp=datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc),
            model_time_speed=base_config.model_time_speed,
            model_timezone=base_config.model_timezone,
            model_t0=base_config.model_t0,
            gen_seed=source_config.seed,
        )
        state_manager = MagicMock()
        state_manager.load.return_value = state

        with patch("clickstream_generator.service.start_http_server"), \
             patch("clickstream_generator.service.ensure_topics"), \
             patch("clickstream_generator.service.KafkaPublisher"), \
             patch("clickstream_generator.service.KafkaBatchHistory"), \
             patch(
                 "clickstream_generator.service.KafkaStateManager",
                 return_value=state_manager,
             ), \
             patch.object(GeneratorService, "_main_loop", return_value=None), \
             caplog.at_level(logging.WARNING, logger="generator"):

            service = GeneratorService(base_config)
            service.start()

        assert service._tick == 0
        assert service._model_time == base_config.model_t0
        assert "state config mismatch: gen_seed" in caplog.text

    def test_state_reset_skips_loading_saved_state(self, base_config):
        """GEN_STATE_RESET=true запускает сервис с чистого состояния."""
        reset_config = replace(base_config, state_reset=True)
        state_manager = MagicMock()

        with patch("clickstream_generator.service.start_http_server"), \
             patch("clickstream_generator.service.ensure_topics"), \
             patch("clickstream_generator.service.KafkaPublisher"), \
             patch("clickstream_generator.service.KafkaBatchHistory"), \
             patch(
                 "clickstream_generator.service.KafkaStateManager",
                 return_value=state_manager,
             ), \
             patch.object(GeneratorService, "_main_loop", return_value=None):

            service = GeneratorService(reset_config)
            service.start()

        state_manager.load.assert_not_called()
        assert service._tick == 0

    def test_invalid_restored_v2_state_starts_fresh(self, base_config, caplog):
        """Сервис не падает, если v2 state ссылается на неизвестный профиль."""
        state_manager = MagicMock()
        state_manager.load.return_value = GeneratorState(
            tick=9,
            rng_state=random.Random(42).getstate(),
            last_batch_id="bad-v2",
            last_timestamp=datetime.now(timezone.utc),
            model_timestamp=base_config.model_t0,
            wall_timestamp=datetime.now(timezone.utc),
            model_time_speed=base_config.model_time_speed,
            model_timezone=base_config.model_timezone,
            model_t0=base_config.model_t0,
            gen_seed=base_config.seed,
            population=[
                {
                    "user_domain_id": "user-unknown",
                    "seed_click_id": "missing-click-id",
                }
            ],
            active_visits=[],
        )

        with patch("clickstream_generator.service.start_http_server"), \
             patch("clickstream_generator.service.ensure_topics"), \
             patch("clickstream_generator.service.KafkaPublisher"), \
             patch("clickstream_generator.service.KafkaBatchHistory"), \
             patch(
                 "clickstream_generator.service.KafkaStateManager",
                 return_value=state_manager,
             ), \
             patch.object(GeneratorService, "_main_loop", return_value=None), \
             caplog.at_level(logging.WARNING, logger="generator"):

            service = GeneratorService(base_config)
            service.start()

        assert service._tick == 0
        assert "State data was invalid" in caplog.text
