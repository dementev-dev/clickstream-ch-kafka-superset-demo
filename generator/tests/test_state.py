"""
Тесты сохранения и восстановления состояния генератора.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from generator import GeneratorState, KafkaStateManager


class TestGeneratorState:
    """Тесты структуры состояния генератора."""

    def test_state_creation(self):
        """Создание состояния с всеми полями."""
        now = datetime.now(timezone.utc)
        rng_state = (3, (1, 2, 3), None)  # Минимальный валидный state для random

        state = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
            version="1.0",
        )

        assert state.tick == 42
        assert state.rng_state == rng_state
        assert state.last_batch_id == "abc123"
        assert state.last_timestamp == now
        assert state.version == "1.0"

    def test_default_version(self):
        """Версия по умолчанию."""
        now = datetime.now(timezone.utc)
        rng_state = (3, (1, 2, 3), None)

        state = GeneratorState(
            tick=1,
            rng_state=rng_state,
            last_batch_id="test",
            last_timestamp=now,
        )

        assert state.version == "1.0"

    def test_to_dict_serialization(self):
        """Сериализация в словарь (JSON-safe, без pickle)."""
        now = datetime.now(timezone.utc)
        rng_state = (3, (1, 2, 3), None)

        state = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
        )

        data = state.to_dict()

        assert data["tick"] == 42
        assert data["last_batch_id"] == "abc123"
        assert data["last_timestamp"] == now.isoformat()
        assert data["version"] == "1.0"

        # Проверяем что rng_state сериализован как tuple (JSON-safe, без pickle)
        assert "rng_state" in data
        assert data["rng_state"] == rng_state
        # Проверяем что можно сериализовать в JSON и восстановить
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored_state = GeneratorState.from_dict(restored_data)
        assert restored_state.rng_state == rng_state

    def test_from_dict_deserialization(self):
        """Десериализация из словаря."""
        now = datetime.now(timezone.utc)
        rng_state = (3, (1, 2, 3), None)

        # Создаём исходное состояние
        original = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
        )

        # Сериализуем и десериализуем
        data = original.to_dict()
        restored = GeneratorState.from_dict(data)

        assert restored.tick == original.tick
        assert restored.rng_state == original.rng_state
        assert restored.last_batch_id == original.last_batch_id
        assert restored.last_timestamp == original.last_timestamp
        assert restored.version == original.version

    def test_roundtrip_with_real_random(self):
        """Проверка что RNG state действительно восстанавливает последовательность."""
        import random

        # Создаём генератор и делаем несколько вызовов
        rng = random.Random(12345)
        values_before = [rng.random() for _ in range(5)]

        # Сохраняем состояние
        state = GeneratorState(
            tick=10,
            rng_state=rng.getstate(),
            last_batch_id="test",
            last_timestamp=datetime.now(timezone.utc),
        )

        # Десериализуем
        data = state.to_dict()
        restored_state = GeneratorState.from_dict(data)

        # Создаём новый генератор с восстановленным состоянием
        new_rng = random.Random()
        new_rng.setstate(restored_state.rng_state)

        # Проверяем что следующие значения совпадают
        values_after = [new_rng.random() for _ in range(5)]

        # Если state восстановлен корректно, values должны совпадать
        # Но т.к. rng уже "прокручен" на 5 значений, берём следующие
        rng.setstate(state.rng_state)
        next_values = [rng.random() for _ in range(5)]

        assert next_values == values_after


class TestKafkaStateManager:
    """Тесты менеджера состояния."""

    def test_init(self):
        """Инициализация менеджера."""
        with patch("generator._import_kafka") as mock_import:
            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            manager = KafkaStateManager("kafka:29092")

            assert manager.bootstrap_servers == "kafka:29092"
            assert manager.STATE_TOPIC == "generator_state"
            assert manager.STATE_KEY == "default"
            mock_producer_class.assert_called_once()

    def test_save(self):
        """Сохранение состояния."""
        with patch("generator._import_kafka") as mock_import:
            mock_producer = MagicMock()
            mock_producer_class = MagicMock(return_value=mock_producer)
            mock_import.return_value = (mock_producer_class, None)

            manager = KafkaStateManager("kafka:29092")

            now = datetime.now(timezone.utc)
            rng_state = (3, (1, 2, 3), None)
            state = GeneratorState(
                tick=42,
                rng_state=rng_state,
                last_batch_id="abc123",
                last_timestamp=now,
            )

            manager.save(state)

            # Проверяем что producer.send был вызван
            mock_producer.send.assert_called_once()
            call_args = mock_producer.send.call_args
            assert call_args[0][0] == "generator_state"  # topic
            assert call_args[1]["key"] == "default"  # key
            assert call_args[1]["value"]["tick"] == 42
            assert call_args[1]["value"]["last_batch_id"] == "abc123"

    def test_load_no_messages(self):
        """Загрузка при отсутствии состояния."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            # Мокаем пустой consumer (нет сообщений)
            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            result = manager.load()

            assert result is None

    def test_load_with_messages(self):
        """Загрузка существующего состояния."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            now = datetime.now(timezone.utc)
            rng_state = (3, (1, 2, 3), None)
            state = GeneratorState(
                tick=100,
                rng_state=rng_state,
                last_batch_id="xyz789",
                last_timestamp=now,
            )

            # Мокаем consumer с сообщением
            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = state.to_dict()

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            result = manager.load()

            assert result is not None
            assert result.tick == 100
            assert result.last_batch_id == "xyz789"

    def test_load_ignores_wrong_key(self):
        """Загрузка игнорирует сообщения с другим ключом."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            # Мокаем consumer с сообщением с неправильным ключом
            mock_message = MagicMock()
            mock_message.key = b"other_generator"  # Другой ключ
            mock_message.value = {"tick": 999}

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            result = manager.load()

            # Не должно быть загружено, т.к. ключ не совпадает
            assert result is None

    def test_load_exception_returns_none(self):
        """При ошибке загрузки возвращается None."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            # Симулируем ошибку при создании consumer
            mock_consumer_class.side_effect = Exception("Kafka unavailable")

            manager = KafkaStateManager("kafka:29092")
            result = manager.load()

            assert result is None

    def test_flush(self):
        """Сброс буфера."""
        with patch("generator._import_kafka") as mock_import:
            mock_producer = MagicMock()
            mock_producer_class = MagicMock(return_value=mock_producer)
            mock_import.return_value = (mock_producer_class, None)

            manager = KafkaStateManager("kafka:29092")
            manager.flush()

            mock_producer.flush.assert_called_once()

    def test_close(self):
        """Закрытие соединения."""
        with patch("generator._import_kafka") as mock_import:
            mock_producer = MagicMock()
            mock_producer_class = MagicMock(return_value=mock_producer)
            mock_import.return_value = (mock_producer_class, None)

            manager = KafkaStateManager("kafka:29092")
            manager.close()

            mock_producer.close.assert_called_once()


class TestJsonSafeState:
    """Тесты JSON-safe сериализации state (без pickle)."""

    def test_json_roundtrip_with_nested_tuples(self):
        """Проверка что nested tuple корректно восстанавливается после JSON."""
        from generator import _nested_list_to_tuple
        
        # Симулируем что получаем после json.loads() - все tuple становятся list
        json_loaded = [3, [1, 2, 3], None]
        
        result = _nested_list_to_tuple(json_loaded)
        
        assert result == (3, (1, 2, 3), None)
        assert isinstance(result, tuple)
        assert isinstance(result[1], tuple)

    def test_json_roundtrip_rng_state(self):
        """Полный цикл: rng.getstate() -> JSON -> from_dict -> setstate."""
        import random
        
        rng = random.Random(42)
        # Делаем несколько вызовов
        values_before = [rng.random() for _ in range(10)]
        
        # Сохраняем state
        state = GeneratorState(
            tick=100,
            rng_state=rng.getstate(),
            last_batch_id="test123",
            last_timestamp=datetime.now(timezone.utc),
        )
        
        # Сериализуем через JSON (как в Kafka)
        data = state.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        
        # Восстанавливаем
        restored_state = GeneratorState.from_dict(restored_data)
        
        # Проверяем что RNG state восстановлен корректно
        rng2 = random.Random()
        rng2.setstate(restored_state.rng_state)
        
        # Проверяем что следующие значения совпадают
        values_after = [rng2.random() for _ in range(5)]
        
        # Оригинальный RNG должен дать те же значения
        values_expected = [rng.random() for _ in range(5)]
        
        assert values_after == values_expected


class TestWithRetry:
    """Тесты функции _with_retry."""

    def test_success_on_first_attempt(self):
        """Успех с первой попытки."""
        from generator import _with_retry
        
        operation = MagicMock(return_value="success")
        
        result = _with_retry(operation, max_retries=3, base_delay=0.01)
        
        assert result == "success"
        assert operation.call_count == 1

    def test_success_after_retries(self):
        """Успех после нескольких попыток."""
        from generator import _with_retry
        
        operation = MagicMock(side_effect=[Exception("fail1"), Exception("fail2"), "success"])
        
        result = _with_retry(operation, max_retries=3, base_delay=0.01)
        
        assert result == "success"
        assert operation.call_count == 3

    def test_failure_after_all_retries(self):
        """Исчерпание всех попыток."""
        from generator import _with_retry
        
        operation = MagicMock(side_effect=Exception("always fails"))
        
        with pytest.raises(Exception, match="always fails"):
            _with_retry(operation, max_retries=3, base_delay=0.01)
        
        assert operation.call_count == 3
