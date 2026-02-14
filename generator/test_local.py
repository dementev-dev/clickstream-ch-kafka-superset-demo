#!/usr/bin/env python3
"""
Локальное тестирование генератора без Kafka.
Просто проверяет логику генерации событий.
"""

import json
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, str(Path(__file__).parent))

from generator import Config, EventDictionary, EventGenerator


def test_generation():
    """Тест генерации событий."""
    print("=" * 60)
    print("Тестирование генератора событий (локально)")
    print("=" * 60)

    # Создаём конфиг с дефолтными значениями
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=10,  # Мало для теста
        jitter_pct=20,
        min_events_per_tick=5,
        max_events_per_tick=20,
        data_dir=Path(__file__).parent.parent / "data",
        seed=42,
        enabled=True,
    )

    print(f"\nКонфигурация:")
    print(f"  data_dir: {config.data_dir}")
    print(f"  seed: {config.seed}")
    print(f"  lambda_base: {config.lambda_base_per_min}")

    # Загружаем словарь
    print(f"\nЗагрузка словаря событий...")
    dictionary = EventDictionary.load(config.data_dir)

    # Создаём генератор
    generator = EventGenerator(dictionary, config)

    # Генерируем несколько батчей
    print(f"\nГенерация тестовых батчей:")
    for i in range(3):
        batch_size = generator._calculate_events_count()
        print(f"\n--- Batch {i + 1} (size={batch_size}) ---")

        batch = generator.generate_batch(batch_size)

        # Проверяем связность
        browser = batch["browser_events"][0]
        event_id = browser["event_id"]
        click_id = browser["click_id"]

        # Location должен иметь тот же event_id
        location = batch["location_events"][0]
        assert location["event_id"] == event_id, "Event ID mismatch!"

        # Device и Geo должны иметь тот же click_id
        device = batch["device_events"][0]
        geo = batch["geo_events"][0]
        assert device["click_id"] == click_id, "Click ID mismatch in device!"
        assert geo["click_id"] == click_id, "Click ID mismatch in geo!"

        # Проверяем, что ID новые (не из оригинальных данных)
        original_event_ids = {e["event_id"] for e in dictionary.browser_events}
        original_click_ids = {e["click_id"] for e in dictionary.browser_events}

        assert event_id not in original_event_ids, "Event ID not regenerated!"
        assert click_id not in original_click_ids, "Click ID not regenerated!"

        # Выводим пример события
        print(f"  Browser event: {json.dumps(browser, indent=2)[:200]}...")
        print(f"  ✓ Связи проверены: event_id={event_id[:8]}..., click_id={click_id[:8]}...")

    print("\n" + "=" * 60)
    print("Все тесты пройдены!")
    print("=" * 60)


if __name__ == "__main__":
    test_generation()
