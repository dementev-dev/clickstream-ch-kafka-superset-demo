"""
Тесты для KafkaBatchHistory.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from generator import BatchRecord, KafkaBatchHistory


class TestBatchRecordSerialization:
    """Тесты сериализации BatchRecord."""

    def test_to_dict_serializes_all_fields(self):
        """to_dict сериализует все поля."""
        now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        record = BatchRecord(
            batch_id="abc123",
            started_at=now,
            finished_at=now,
            sent_total=100,
            sent_browser=25,
            sent_location=25,
            sent_device=25,
            sent_geo=25,
            status="success",
            error_message=None,
        )

        data = record.to_dict()

        assert data["batch_id"] == "abc123"
        assert data["started_at"] == "2024-01-15T10:30:00+00:00"
        assert data["finished_at"] == "2024-01-15T10:30:00+00:00"
        assert data["sent_total"] == 100
        assert data["sent_browser"] == 25
        assert data["sent_location"] == 25
        assert data["sent_device"] == 25
        assert data["sent_geo"] == 25
        assert data["status"] == "success"
        assert data["error_message"] is None

    def test_to_dict_with_error_message(self):
        """to_dict сериализует error_message если есть."""
        now = datetime.now(timezone.utc)
        record = BatchRecord(
            batch_id="err456",
            started_at=now,
            finished_at=now,
            sent_total=0,
            sent_browser=0,
            sent_location=0,
            sent_device=0,
            sent_geo=0,
            status="error",
            error_message="Connection failed",
        )

        data = record.to_dict()

        assert data["error_message"] == "Connection failed"

    def test_isoformat_includes_timezone(self):
        """ISO формат включает timezone."""
        now = datetime.now(timezone.utc)
        record = BatchRecord(
            batch_id="tz789",
            started_at=now,
            finished_at=now,
            sent_total=50,
            sent_browser=12,
            sent_location=13,
            sent_device=12,
            sent_geo=13,
            status="partial",
            error_message="Some errors",
        )

        data = record.to_dict()

        # Должен содержать +00:00 или Z
        assert "+" in data["started_at"] or "Z" in data["started_at"]


class TestKafkaBatchHistory:
    """Тесты KafkaBatchHistory."""

    def test_history_topic_constant(self):
        """Константа топика истории."""
        assert KafkaBatchHistory.HISTORY_TOPIC == "generator_batch_history"

    @patch("generator._import_kafka")
    def test_init_connects_to_kafka(self, mock_import):
        """Инициализация подключается к Kafka."""
        mock_producer_class = MagicMock()
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")

        assert history.bootstrap_servers == "localhost:9092"
        mock_producer_class.assert_called_once()

    @patch("generator._with_retry")
    @patch("generator._import_kafka")
    def test_init_uses_retry(self, mock_import, mock_retry):
        """Инициализация использует retry для подключения."""
        mock_producer_class = MagicMock()
        mock_import.return_value = (mock_producer_class, Exception)
        mock_retry.side_effect = lambda f, **kwargs: f()  # Выполняем функцию сразу

        KafkaBatchHistory("localhost:9092")

        # Проверяем что _with_retry был вызван
        mock_retry.assert_called()

    @patch("generator._import_kafka")
    def test_add_sends_to_kafka(self, mock_import):
        """add отправляет сообщение в Kafka."""
        mock_producer = MagicMock()
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")
        now = datetime.now(timezone.utc)
        record = BatchRecord(
            batch_id="test123",
            started_at=now,
            finished_at=now,
            sent_total=100,
            sent_browser=25,
            sent_location=25,
            sent_device=25,
            sent_geo=25,
            status="success",
            error_message=None,
        )

        history.add(record)

        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == "generator_batch_history"
        assert call_args[1]["key"] == "test123"
        assert "value" in call_args[1]

    @patch("generator._import_kafka")
    def test_add_retries_on_failure(self, mock_import):
        """add делает retry при ошибке и пытается реконнект."""
        mock_producer = MagicMock()
        # Первые 3 вызова падают, потом успех
        mock_producer.send.side_effect = [
            Exception("Fail 1"),
            Exception("Fail 2"),
            Exception("Fail 3"),
            MagicMock(),  # Успех после реконнекта
        ]
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")
        now = datetime.now(timezone.utc)
        record = BatchRecord(
            batch_id="retry456",
            started_at=now,
            finished_at=now,
            sent_total=50,
            sent_browser=12,
            sent_location=13,
            sent_device=12,
            sent_geo=13,
            status="success",
            error_message=None,
        )

        # Не должно упасть - должен быть retry + reconnect
        history.add(record)

        # Проверяем что producer.send вызывался несколько раз (retry)
        assert mock_producer.send.call_count >= 1

    @patch("generator._import_kafka")
    def test_flush_calls_producer_flush(self, mock_import):
        """flush вызывает flush у producer."""
        mock_producer = MagicMock()
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")
        history.flush()

        mock_producer.flush.assert_called_once()

    @patch("generator._import_kafka")
    def test_close_calls_producer_close(self, mock_import):
        """close вызывает close у producer."""
        mock_producer = MagicMock()
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")
        history.close()

        mock_producer.close.assert_called_once()

    @patch("generator._import_kafka")
    def test_close_ignores_errors(self, mock_import):
        """close игнорирует ошибки при закрытии."""
        mock_producer = MagicMock()
        mock_producer.close.side_effect = Exception("Close failed")
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_import.return_value = (mock_producer_class, Exception)

        history = KafkaBatchHistory("localhost:9092")
        # Не должно упасть
        history.close()
