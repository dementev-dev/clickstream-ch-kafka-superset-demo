"""
Тесты генерации событий.
"""

import uuid
from datetime import datetime

import pytest
from generator import EventGenerator, EventDictionary


class TestEventGeneration:
    """Тесты генерации событий."""

    def test_event_dictionary_loads(self, event_dictionary):
        """Словарь событий загружается корректно."""
        assert len(event_dictionary.browser_events) == 1000
        assert len(event_dictionary.location_events) == 1000
        assert len(event_dictionary.device_events) == 1000
        assert len(event_dictionary.geo_events) == 1000

    def test_dictionary_consistency(self, event_dictionary):
        """Все записи в словаре имеют корректные связи."""
        browser_event_ids = {e["event_id"] for e in event_dictionary.browser_events}
        browser_click_ids = {e["click_id"] for e in event_dictionary.browser_events}

        location_orphaned = sum(
            1 for loc in event_dictionary.location_events
            if loc["event_id"] not in browser_event_ids
        )
        device_orphaned = sum(
            1 for dev in event_dictionary.device_events
            if dev["click_id"] not in browser_click_ids
        )
        geo_orphaned = sum(
            1 for geo in event_dictionary.geo_events
            if geo["click_id"] not in browser_click_ids
        )

        assert location_orphaned == 0, f"Found {location_orphaned} orphaned location records"
        assert device_orphaned == 0, f"Found {device_orphaned} orphaned device records"
        assert geo_orphaned == 0, f"Found {geo_orphaned} orphaned geo records"

    def test_generate_batch_structure(self, event_dictionary, base_config):
        """Батч имеет правильную структуру."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(10)

        assert "browser_events" in batch
        assert "location_events" in batch
        assert "device_events" in batch
        assert "geo_events" in batch

    def test_generate_batch_size(self, event_dictionary, base_config):
        """Батч содержит правильное количество событий."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(10)

        assert len(batch["browser_events"]) == 10
        assert len(batch["location_events"]) == 10
        assert len(batch["device_events"]) == 10
        assert len(batch["geo_events"]) == 10

    def test_event_ids_are_new_uuids(self, event_dictionary, base_config):
        """event_id и click_id — новые UUID, не из оригинальных данных."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        original_event_ids = {e["event_id"] for e in event_dictionary.browser_events}
        original_click_ids = {e["click_id"] for e in event_dictionary.browser_events}

        browser_event = batch["browser_events"][0]
        assert browser_event["event_id"] not in original_event_ids
        assert browser_event["click_id"] not in original_click_ids

        # Проверяем, что это валидный UUID
        uuid.UUID(browser_event["event_id"])
        uuid.UUID(browser_event["click_id"])

    def test_event_timestamp_format(self, event_dictionary, base_config):
        """event_timestamp имеет правильный формат."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        browser_event = batch["browser_events"][0]
        timestamp = browser_event["event_timestamp"]

        # Должен парситься как datetime
        dt = datetime.fromisoformat(timestamp.replace(" ", "T"))
        assert dt.year >= 2024

    def test_links_consistency(self, event_dictionary, base_config):
        """Связи между событиями сохраняются."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(5)

        for i, browser in enumerate(batch["browser_events"]):
            event_id = browser["event_id"]
            click_id = browser["click_id"]

            # Location должен иметь тот же event_id
            assert batch["location_events"][i]["event_id"] == event_id

            # Device и Geo должны иметь тот же click_id
            assert batch["device_events"][i]["click_id"] == click_id
            assert batch["geo_events"][i]["click_id"] == click_id

    def test_required_fields_present(self, event_dictionary, base_config):
        """Все обязательные поля присутствуют в событиях."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        browser = batch["browser_events"][0]
        required_fields = [
            "event_id", "event_timestamp", "event_type", "click_id",
            "browser_name", "browser_user_agent", "browser_language"
        ]

        for field in required_fields:
            assert field in browser, f"Missing field: {field}"


class TestPoissonDistribution:
    """Тесты статистической модели."""

    def test_calculate_events_respects_bounds(self, event_dictionary, base_config):
        """Расчет количества событий уважает границы."""
        generator = EventGenerator(event_dictionary, base_config)

        samples = [generator._calculate_events_count() for _ in range(100)]

        assert all(s >= base_config.min_events_per_tick for s in samples)
        assert all(s <= base_config.max_events_per_tick for s in samples)

    def test_jitter_increases_variance(self, event_dictionary, base_config, config_no_jitter):
        """Jitter увеличивает дисперсию."""
        gen_with = EventGenerator(event_dictionary, base_config)
        gen_without = EventGenerator(event_dictionary, config_no_jitter)

        samples_with = [gen_with._calculate_events_count() for _ in range(200)]
        samples_without = [gen_without._calculate_events_count() for _ in range(200)]

        mean_with = sum(samples_with) / len(samples_with)
        mean_without = sum(samples_without) / len(samples_without)

        var_with = sum((x - mean_with) ** 2 for x in samples_with) / len(samples_with)
        var_without = sum((x - mean_without) ** 2 for x in samples_without) / len(samples_without)

        assert var_with > var_without, \
            f"Jitter should increase variance: {var_with} vs {var_without}"

    def test_mean_is_reasonable(self, event_dictionary, base_config):
        """Среднее значение в разумных пределах."""
        generator = EventGenerator(event_dictionary, base_config)

        samples = [generator._calculate_events_count() for _ in range(500)]
        mean = sum(samples) / len(samples)

        # Ожидаем: lambda_base * tick_seconds / 60 * hour_factor
        # hour_factor обычно 0.7-1.2
        expected_base = base_config.lambda_base_per_min * base_config.tick_seconds / 60.0

        # Допустимое отклонение до 50%
        assert mean > expected_base * 0.5, f"Mean {mean} too low (expected ~{expected_base})"
        assert mean < expected_base * 1.5, f"Mean {mean} too high (expected ~{expected_base})"


class TestEmptyData:
    """Тесты обработки пустых данных."""

    def test_empty_jsonl_raises_error(self, empty_temp_dir):
        """Пустые JSONL файлы вызывают ValueError при загрузке."""
        with pytest.raises(ValueError, match="browser_events.jsonl is empty"):
            EventDictionary.load(empty_temp_dir)
