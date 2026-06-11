"""
Тесты для GeneratorService и интеграционных сценариев.
"""

import logging
import random
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from generator import (
    Config,
    EventDictionary,
    EventGenerator,
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

    def test_save_state_writes_tick_stream_state_v2(self, base_config):
        """Сервис сохраняет v2-снимок тикового слоя."""
        service = GeneratorService(base_config)
        service.state_manager = MagicMock()
        tick_at = datetime.now(timezone.utc).replace(tzinfo=None)
        service.stream.generate_tick(event_budget=10, tick_started_at=tick_at)
        service._tick = 1

        service._save_state("batch-1")

        saved_state = service.state_manager.save.call_args.args[0]
        assert saved_state.version == "2.0"
        assert saved_state.population
        assert saved_state.active_visits
        service.state_manager.flush.assert_called_once()

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
