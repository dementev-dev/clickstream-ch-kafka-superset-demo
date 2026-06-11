"""
Тесты сохранения и восстановления состояния генератора.
"""
import json
import logging
import random
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from generator import GeneratorState, KafkaStateManager


def _make_valid_rng_state(seed: int = 42):
    """Создаёт валидный RNG state для тестов."""
    rng = random.Random(seed)
    return rng.getstate()


def _make_valid_v2_state_data() -> dict:
    """Создаёт минимальный валидный state v2 для тестов загрузки."""
    return {
        "tick": 42,
        "rng_state": list(_make_valid_rng_state(42)),
        "last_batch_id": "v2",
        "last_timestamp": "2026-06-11T12:00:00+00:00",
        "version": "2.0",
        "population": [
            {
                "user_domain_id": "user-1",
                "seed_click_id": "seed-1",
                "active_click_id": "visit-1",
                "last_finished_at": None,
            }
        ],
        "active_visits": [
            {
                "user_domain_id": "user-1",
                "click_id": "visit-1",
                "next_index": 1,
                "started_at": "2026-06-11T12:00:00",
                "offsets_us": [0, 60_000_000],
                "page_url_paths": ["/home", "/cart"],
            }
        ],
        "pending_visit_births": 0.5,
    }


def _minimal_population() -> list[dict]:
    return [
        {
            "user_domain_id": "user-1",
            "seed_click_id": "seed-1",
            "active_click_id": None,
            "last_finished_at": None,
        }
    ]


class TestGeneratorState:
    """Тесты структуры состояния генератора."""

    def test_state_creation(self):
        """Создание состояния с всеми полями."""
        now = datetime.now(timezone.utc)
        rng_state = _make_valid_rng_state(42)

        state = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
            version="2.0",
        )

        assert state.tick == 42
        assert state.rng_state == rng_state
        assert state.last_batch_id == "abc123"
        assert state.last_timestamp == now
        assert state.version == "2.0"

    def test_default_version(self):
        """Новые состояния по умолчанию пишутся в версии 2."""
        now = datetime.now(timezone.utc)
        rng_state = _make_valid_rng_state(42)

        state = GeneratorState(
            tick=1,
            rng_state=rng_state,
            last_batch_id="test",
            last_timestamp=now,
        )

        assert state.version == "2.0"

    def test_to_dict_serialization(self):
        """Сериализация в словарь (JSON-safe, без pickle)."""
        now = datetime.now(timezone.utc)
        rng_state = _make_valid_rng_state(42)

        state = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
            population=_minimal_population(),
        )

        data = state.to_dict()

        assert data["tick"] == 42
        assert data["last_batch_id"] == "abc123"
        assert data["last_timestamp"] == now.isoformat()
        assert data["version"] == "2.0"

        # Проверяем что rng_state сериализован как tuple (JSON-safe, без pickle)
        assert "rng_state" in data
        # После to_dict rng_state должен быть tuple
        assert isinstance(data["rng_state"], tuple)
        # Проверяем что можно сериализовать в JSON и восстановить
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored_state = GeneratorState.from_dict(restored_data)
        assert restored_state.rng_state == rng_state

    def test_from_dict_deserialization(self):
        """Десериализация из словаря."""
        now = datetime.now(timezone.utc)
        rng_state = _make_valid_rng_state(42)

        # Создаём исходное состояние
        original = GeneratorState(
            tick=42,
            rng_state=rng_state,
            last_batch_id="abc123",
            last_timestamp=now,
            population=_minimal_population(),
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
        # Создаём генератор и делаем несколько вызовов
        rng = random.Random(12345)
        values_before = [rng.random() for _ in range(5)]

        # Сохраняем состояние
        state = GeneratorState(
            tick=10,
            rng_state=rng.getstate(),
            last_batch_id="test",
            last_timestamp=datetime.now(timezone.utc),
            population=_minimal_population(),
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

    def test_version_2_roundtrip_keeps_population_and_active_visits(self):
        """State v2 хранит популяцию и активные визиты в JSON."""
        rng_state = _make_valid_rng_state(42)
        state = GeneratorState(
            tick=7,
            rng_state=rng_state,
            last_batch_id="batch-7",
            last_timestamp=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
            version="2.0",
            population=[
                {
                    "user_domain_id": "user-1",
                    "seed_click_id": "seed-1",
                    "active_click_id": "visit-1",
                    "last_finished_at": "2026-06-11T11:30:00",
                }
            ],
            active_visits=[
                {
                    "user_domain_id": "user-1",
                    "click_id": "visit-1",
                    "next_index": 1,
                    "started_at": "2026-06-11T12:00:00",
                    "offsets_us": [0, 60_000_000],
                    "page_url_paths": ["/home", "/cart"],
                }
            ],
            pending_visit_births=0.5,
        )

        restored = GeneratorState.from_dict(json.loads(json.dumps(state.to_dict())))

        assert restored.version == "2.0"
        assert restored.tick == state.tick
        assert restored.rng_state == rng_state
        assert restored.population == state.population
        assert restored.active_visits == state.active_visits
        assert restored.pending_visit_births == 0.5


class TestGeneratorStateValidation:
    """Тесты валидации состояния и graceful degradation."""

    def test_from_dict_missing_version_raises(self):
        """from_dict выбрасывает исключение при отсутствии версии v2."""
        data = {
            "tick": 42,
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
        }

        with pytest.raises(ValueError, match="version"):
            GeneratorState.from_dict(data)

    def test_from_dict_invalid_rng_state_raises(self):
        """from_dict выбрасывает исключение при невалидном rng_state."""
        data = {
            "tick": 42,
            "rng_state": "not_a_tuple",
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
            "version": "2.0",
            "population": _minimal_population(),
            "active_visits": [],
        }

        with pytest.raises(ValueError):
            GeneratorState.from_dict(data)

    def test_from_dict_insufficient_rng_state_raises(self):
        """from_dict выбрасывает исключение при коротком rng_state."""
        data = {
            "tick": 42,
            "rng_state": [1],  # Слишком короткий
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
            "version": "2.0",
            "population": _minimal_population(),
            "active_visits": [],
        }

        with pytest.raises(ValueError):
            GeneratorState.from_dict(data)

    def test_from_dict_invalid_setstate_raises(self):
        """from_dict выбрасывает исключение если setstate падает."""
        data = {
            "tick": 42,
            "rng_state": [999, [1, 2, 3], None],  # Невалидный state
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
            "version": "2.0",
            "population": [],
            "active_visits": [],
        }

        with pytest.raises(ValueError):
            GeneratorState.from_dict(data)

    def test_from_dict_safe_returns_none_on_invalid(self):
        """from_dict_safe возвращает None при невалидных данных."""
        data = {
            "tick": 42,
            "rng_state": "invalid",
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
            "version": "2.0",
            "population": [],
            "active_visits": [],
        }

        result = GeneratorState.from_dict_safe(data)
        assert result is None

    def test_from_dict_safe_returns_state_on_valid(self):
        """from_dict_safe возвращает state при валидных данных."""
        rng = random.Random(42)
        data = {
            "tick": 42,
            "rng_state": list(rng.getstate()),  # JSON сериализует tuple как list
            "last_batch_id": "test",
            "last_timestamp": "2024-01-01T00:00:00+00:00",
            "version": "2.0",
            "population": _minimal_population(),
            "active_visits": [],
        }

        result = GeneratorState.from_dict_safe(data)
        assert result is not None
        assert result.tick == 42

    def test_from_dict_rejects_old_state_without_version(self):
        """from_dict не восстанавливает старое state v1 без версии."""
        rng = random.Random(42)
        data = {
            "rng_state": list(rng.getstate()),
        }

        with pytest.raises(ValueError, match="version"):
            GeneratorState.from_dict(data)


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
            state = GeneratorState(
                tick=42,
                rng_state=_make_valid_rng_state(42),
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
            state = GeneratorState(
                tick=100,
                rng_state=_make_valid_rng_state(100),
                last_batch_id="xyz789",
                last_timestamp=now,
                version="2.0",
                population=_minimal_population(),
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

    def test_load_invalid_state_returns_none(self):
        """Загрузка невалидного state возвращает None (graceful degradation)."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            # Мокаем consumer с невалидным сообщением
            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = {"tick": 42, "rng_state": "invalid"}

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            result = manager.load()

            # Должно вернуть None из-за невалидного state
            assert result is None

    def test_load_invalid_v2_nested_state_returns_none(self, caplog):
        """Битое state v2 с валидным rng_state даёт чистый старт."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = {
                "tick": 42,
                "rng_state": list(_make_valid_rng_state(42)),
                "last_batch_id": "bad-v2",
                "last_timestamp": "2026-06-11T12:00:00+00:00",
                "version": "2.0",
                "population": [{"user_domain_id": "user-1"}],
                "active_visits": [
                    {
                        "user_domain_id": "user-1",
                        "click_id": "visit-1",
                        "next_index": 1,
                        "started_at": "2026-06-11T12:00:00",
                        "offsets_us": [0],
                    }
                ],
            }

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_empty_population_v2_returns_none(self, caplog):
        """Пустая популяция в state v2 не восстанавливается."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            bad_state = _make_valid_v2_state_data()
            bad_state["population"] = []
            bad_state["active_visits"] = []

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = bad_state

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_bad_pending_births_v2_returns_none(self, caplog):
        """Нечисловой pending_visit_births в state v2 не восстанавливается."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            bad_state = _make_valid_v2_state_data()
            bad_state["pending_visit_births"] = "bad"

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = bad_state

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_active_visit_with_unknown_user_v2_returns_none(self, caplog):
        """Активный визит должен ссылаться на пользователя из популяции."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            bad_state = _make_valid_v2_state_data()
            bad_state["active_visits"][0]["user_domain_id"] = "missing-user"

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = bad_state

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_active_visit_with_conflicting_click_id_v2_returns_none(self, caplog):
        """active_click_id пользователя не должен противоречить визиту."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            bad_state = _make_valid_v2_state_data()
            bad_state["population"][0]["active_click_id"] = "other-visit"

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = bad_state

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_population_ghost_active_click_id_v2_returns_none(self, caplog):
        """active_click_id пользователя должен иметь соответствующий активный визит."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            bad_state = _make_valid_v2_state_data()
            bad_state["population"][0]["active_click_id"] = "ghost"
            bad_state["active_visits"] = []

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = bad_state

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "Invalid state" in caplog.text

    def test_load_version_1_state_returns_none_with_warning(self, caplog):
        """Старое state v1 не восстанавливается и даёт чистый старт."""
        with patch("generator._import_kafka") as mock_import, \
             patch("kafka.KafkaConsumer") as mock_consumer_class:

            mock_producer_class = MagicMock()
            mock_import.return_value = (mock_producer_class, None)

            old_state = GeneratorState(
                tick=100,
                rng_state=_make_valid_rng_state(100),
                last_batch_id="old",
                last_timestamp=datetime.now(timezone.utc),
                version="1.0",
            )

            mock_message = MagicMock()
            mock_message.key = b"default"
            mock_message.value = old_state.to_dict()

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_consumer_class.return_value = mock_consumer

            manager = KafkaStateManager("kafka:29092")
            with caplog.at_level(logging.WARNING, logger="generator"):
                result = manager.load()

            assert result is None
            assert "version" in caplog.text

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
        rng = random.Random(42)
        # Делаем несколько вызовов
        values_before = [rng.random() for _ in range(10)]
        
        # Сохраняем state
        state = GeneratorState(
            tick=100,
            rng_state=rng.getstate(),
            last_batch_id="test123",
            last_timestamp=datetime.now(timezone.utc),
            population=_minimal_population(),
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
