"""
Тесты для GeneratorService и интеграционных сценариев.
"""

from unittest.mock import MagicMock, patch

import pytest
from generator import (
    Config, EventDictionary, GeneratorService, KafkaBatchHistory
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
